using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;
using VpnHood.Core.TcpStack.Primitives;
using VpnHood.Core.Toolkit.Logging;

namespace VpnHood.Core.TcpStack;

internal sealed class LocalTcpConnection(
    IpEndPointQuad endPointQuad,
    uint isnLocal,
    uint isnRemote,
    ushort? peerMss,
    LocalTcpListener listener,
    TimeSpan? tcpTimeout = null)
    : IDisposable
{
    // For loopback we use a moderate fixed window size; the pipe handles backpressure internally.
    private const ushort LoopbackWindowSize = 16384;

    // Conservative fallback when peer SYN does not advertise an MSS.
    private const ushort DefaultMss = 536;

    // Upper cap so a single TCP segment never exceeds what fits comfortably in a typical MTU.
    private const ushort MaxMss = LoopbackWindowSize;

    private readonly TimeSpan _idleTimeout = tcpTimeout ?? TimeSpan.FromMinutes(15);
    private static readonly TimeSpan IdleCheckInterval = TimeSpan.FromMinutes(1);

    // Pipe options - moderate buffer for loopback. Both pipes use a single producer / single consumer.
    private static readonly PipeOptions PipeOpts = new(
        pauseWriterThreshold: LoopbackWindowSize,
        resumeWriterThreshold: LoopbackWindowSize / 2,
        useSynchronizationContext: false);

    // Pipe for network -> app data (stream reads)
    private readonly Pipe _netToAppPipe = new(PipeOpts);

    // Pipe for app -> network data (stream writes)
    private readonly Pipe _appToNetPipe = new(PipeOpts);

    private readonly Lock _seqLock = new();
    private readonly CancellationTokenSource _cts = new();
    private LocalTcpStream? _pendingStream;

    private bool _finSent;
    private bool _finReceived;
    private bool _sndNxtAfterSynSet;
    private bool _disposed;
    private int _closedFlag;
    private long _lastActivityTicks = Stopwatch.GetTimestamp();
    private bool _netToAppCompleted;
    private bool _appToNetCompleted;
    private Task? _emitTask;
    private uint _sndNxt = isnLocal; // SYN sequence; bumped to ISN+1 after SYN-ACK is sent.
    private uint _rcvNxt = isnRemote + 1; // We have already "consumed" the peer's SYN.

    public IpEndPointQuad EndPointQuad { get; } = endPointQuad;
    public uint IsnLocal { get; } = isnLocal;
    public ushort Mss { get; } = ClampMss(peerMss);
    public TcpConnectionState State { get; private set; } = TcpConnectionState.SynReceived;

    /// <summary>
    /// PipeReader for reading data received from network (used by LocalTcpStream)
    /// </summary>
    public PipeReader NetToAppReader => _netToAppPipe.Reader;

    /// <summary>
    /// Event raised when connection is fully closed and should be removed from the stack.
    /// </summary>
    public event Action<LocalTcpConnection>? OnClosed;

    private static ushort ClampMss(ushort? peerMss)
    {
        if (peerMss is null or 0) return DefaultMss;
        var v = peerMss.Value;
        if (v < 64) return 64;          // pathological lower bound
        if (v > MaxMss) return MaxMss;
        return v;
    }

    /// <summary>
    /// Starts background tasks for this connection.
    /// </summary>
    public void Start(LocalTcpStack stack)
    {
        // Pre-create the stream that will be enqueued once handshake completes.
        _pendingStream = new LocalTcpStream(this, stack);

        // Start idle monitor
        _ = Task.Run(MonitorIdleAsync);

        // Start the connection's data pump
        _emitTask = Task.Run(() => EmitPendingAsync(stack));
    }

    /// <summary>
    /// Gracefully closes the connection: stops accepting new app data, waits for the emit pump
    /// to drain any queued bytes into TCP segments, then sends FIN. Used by
    /// <see cref="LocalTcpStream.DisposeAsync"/> so closing the stream does not truncate data.
    /// </summary>
    public async Task GracefulCloseAsync(LocalTcpStack stack)
    {
        if (_disposed) return;

        // Stop accepting more app data; the emit pump will exit naturally when the buffer
        // is drained and the writer is completed.
        CompleteAppToNet();

        // Wait for the emit pump to finish so all buffered bytes are turned into TCP segments
        // before we emit FIN. Bound the wait so a stuck pump can't hang dispose.
        var emitTask = _emitTask;
        if (emitTask != null) {
            try {
                await emitTask.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch {
                // Timed out or pump faulted: proceed with FIN anyway.
            }
        }

        // Now safe to FIN.
        TryStartFin(stack);
    }

    /// <summary>
    /// Sets SndNxt to ISN + 1 the first time a SYN-ACK is sent. Idempotent for SYN retransmits.
    /// </summary>
    public void SetSndNxtAfterSyn()
    {
        lock (_seqLock)
        {
            if (_sndNxtAfterSynSet) return;
            _sndNxt = IsnLocal + 1;
            _sndNxtAfterSynSet = true;
        }
    }

    /// <summary>
    /// Transitions from SynReceived to Established and hands the pending stream to the listener.
    /// Idempotent: subsequent calls are no-ops.
    /// </summary>
    public void MarkEstablished()
    {
        LocalTcpStream? streamToEnqueue;
        lock (_seqLock)
        {
            if (State != TcpConnectionState.SynReceived) return;
            State = TcpConnectionState.Established;
            streamToEnqueue = _pendingStream;
            _pendingStream = null;
        }

        if (streamToEnqueue != null && !listener.TryEnqueueAccept(streamToEnqueue))
        {
            // Listener has been stopped: dispose stream and reset the connection
            streamToEnqueue.Dispose();
        }
    }

    public (uint sndNxt, uint rcvNxt) SnapshotSequence()
    {
        lock (_seqLock)
        {
            return (_sndNxt, _rcvNxt);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try { _cts.Cancel(); } catch { /* ignore */ }
        // Note: do NOT dispose _cts here. Background tasks may still observe the token after
        // cancellation; CTS dispose is racy with WaitForNextTickAsync. The CTS is cheap and
        // will be GC'd once tasks complete.
    }

    /// <summary>
    /// Writes data from app to the network pipe
    /// </summary>
    public async ValueTask SendAppDataAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        if (_disposed || _appToNetCompleted) return;
        Touch();

        try
        {
            await _appToNetPipe.Writer.WriteAsync(data, ct);
        }
        catch (OperationCanceledException) when (_disposed || ct.IsCancellationRequested)
        {
            // Expected on close / cancel
        }
        catch (InvalidOperationException)
        {
            // Writer was completed - connection is shutting down
        }
    }

    /// <summary>
    /// Handles incoming TCP data from network.
    /// Returns: (handled, needsAck) - handled indicates if packet was processed, needsAck if ACK should be sent
    /// </summary>
    public (bool handled, bool needsAck) TryHandleIncoming(uint seq, uint ack, TcpFlags flags, ReadOnlySpan<byte> payload)
    {
        _ = ack;
        if (_disposed) return (false, false);
        Touch();

        if (flags.HasFlag(TcpFlags.Rst))
        {
            Close();
            return (false, false);
        }

        try
        {
            bool needsAck;
            bool finCloses;

            lock (_seqLock)
            {
                var seqDiff = (long)seq - _rcvNxt;

                if (seqDiff < 0)
                {
                    // Retransmission: ACK without duplicating data.
                    var retransmitEnd = seq + (uint)payload.Length;
                    if (retransmitEnd > _rcvNxt && payload.Length > 0)
                    {
                        var overlap = (int)(_rcvNxt - seq);
                        var newData = payload[overlap..];
                        if (newData.Length > 0)
                        {
                            WriteToAppPipe(newData);
                            _rcvNxt += (uint)newData.Length;
                        }
                    }
                    return (true, true);
                }

                if (seqDiff > 0)
                {
                    // Out of order - send duplicate ACK to trigger fast retransmit
                    return (true, true);
                }

                // seq == _rcvNxt: in-order packet
                if (payload.Length > 0)
                {
                    WriteToAppPipe(payload);
                    _rcvNxt += (uint)payload.Length;
                }

                needsAck = payload.Length > 0;
                finCloses = false;

                if (flags.HasFlag(TcpFlags.Fin))
                {
                    _rcvNxt += 1;
                    _finReceived = true;
                    needsAck = true;

                    // Check if both sides have sent FIN
                    if (_finSent)
                    {
                        State = TcpConnectionState.Closed;
                        finCloses = true;
                    }
                    else
                    {
                        State = TcpConnectionState.Closing;
                    }
                }
            }

            if (flags.HasFlag(TcpFlags.Fin))
            {
                CompleteNetToApp();
                if (finCloses)
                    Close();
            }

            return (true, needsAck);
        }
        catch (InvalidOperationException)
        {
            // Pipe was completed/broken - close connection
            Close();
            return (false, false);
        }
    }

    private void WriteToAppPipe(ReadOnlySpan<byte> data)
    {
        if (_disposed || _netToAppCompleted) return;

        var span = _netToAppPipe.Writer.GetSpan(data.Length);
        data.CopyTo(span);
        _netToAppPipe.Writer.Advance(data.Length);

        // Synchronous-style flush; we discard the ValueTask intentionally because:
        //  - On loopback, the reader (the application) drains the pipe quickly.
        //  - Awaiting here would require restructuring TryHandleIncoming as async,
        //    and we're called from the packet-receive critical path.
        // PauseWriterThreshold still bounds memory because every Pipe segment respects it
        // when GetSpan/Advance are paired with FlushAsync; for very large bursts the writer
        // is observably "busy" via UnflushedBytes which we can revisit if needed.
        _ = _netToAppPipe.Writer.FlushAsync();
    }

    private void Touch()
    {
        Interlocked.Exchange(ref _lastActivityTicks, Stopwatch.GetTimestamp());
    }

    private async Task MonitorIdleAsync()
    {
        try
        {
            using var timer = new PeriodicTimer(IdleCheckInterval);
            while (await timer.WaitForNextTickAsync(_cts.Token))
            {
                if (_disposed || State == TcpConnectionState.Closed)
                    break;

                var last = Interlocked.Read(ref _lastActivityTicks);
                var elapsed = Stopwatch.GetElapsedTime(last);
                if (elapsed >= _idleTimeout)
                {
                    Close();
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on disposal/close
        }
    }

    /// <summary>
    /// Reads from app pipe and emits TCP packets to the network, segmented by negotiated MSS.
    /// </summary>
    private async Task EmitPendingAsync(LocalTcpStack stack)
    {
        var reader = _appToNetPipe.Reader;
        try
        {
            while (!_disposed && State != TcpConnectionState.Closed)
            {
                var result = await reader.ReadAsync(_cts.Token);
                var buffer = result.Buffer;

                if (buffer.IsEmpty)
                {
                    if (result.IsCompleted) break;
                    reader.AdvanceTo(buffer.End);
                    continue;
                }

                var consumed = EmitBuffer(stack, ref buffer);
                reader.AdvanceTo(consumed);

                if (result.IsCompleted && buffer.IsEmpty) break;
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on close
        }
        catch (Exception ex)
        {
            VhLogger.Instance.LogError(ex, "Exception in EmitPendingAsync for connection {EndPoint}", EndPointQuad);
        }
        finally
        {
            try { await reader.CompleteAsync(); } catch { /* ignore */ }
        }
    }

    /// <summary>
    /// Sends as much data as possible from the buffer in MSS-sized segments.
    /// Returns the SequencePosition representing the consumed prefix.
    /// </summary>
    private SequencePosition EmitBuffer(LocalTcpStack stack, ref ReadOnlySequence<byte> buffer)
    {
        var mss = Mss;
        var remaining = buffer;
        while (!remaining.IsEmpty)
        {
            var segLen = (int)Math.Min(remaining.Length, mss);
            var segment = remaining.Slice(0, segLen);

            var tcpPacket = BuildDataPacket(segment, out var tcp);

            uint seqForSegment;
            uint ackForSegment;
            lock (_seqLock)
            {
                seqForSegment = _sndNxt;
                ackForSegment = _rcvNxt;
                _sndNxt += (uint)segLen;
            }

            tcp.SequenceNumber = seqForSegment;
            tcp.AcknowledgmentNumber = ackForSegment;
            tcp.Acknowledgment = true;
            tcp.WindowSize = LoopbackWindowSize;

            // Set PSH on the last segment of the current burst (helps interactive responsiveness).
            if (remaining.Length == segLen)
                tcp.Push = true;

            stack.SendPacket(tcpPacket);

            remaining = remaining.Slice(segLen);
        }

        return buffer.End;
    }

    /// <summary>
    /// Builds a TCP packet whose payload contains the bytes of <paramref name="segment"/>.
    /// Uses ArrayPool when the segment spans multiple buffer chunks.
    /// </summary>
    private IpPacket BuildDataPacket(ReadOnlySequence<byte> segment, out TcpPacket tcp)
    {
        IpPacket packet;
        if (segment.IsSingleSegment)
        {
            packet = PacketBuilder.BuildTcp(EndPointQuad.Destination, EndPointQuad.Source,
                options: ReadOnlySpan<byte>.Empty, payload: segment.FirstSpan);
        }
        else
        {
            var len = (int)segment.Length;
            var rented = ArrayPool<byte>.Shared.Rent(len);
            try
            {
                var span = rented.AsSpan(0, len);
                segment.CopyTo(span);
                packet = PacketBuilder.BuildTcp(EndPointQuad.Destination, EndPointQuad.Source,
                    options: ReadOnlySpan<byte>.Empty, payload: span);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        tcp = packet.ExtractTcp();
        return packet;
    }

    public void StartFin(LocalTcpStack stack)
    {
        IpPacket? finPacket;
        bool closeAfter;

        lock (_seqLock)
        {
            if (_finSent) return;
            _finSent = true;

            CompleteAppToNet();

            finPacket = PacketBuilder.BuildTcp(
                EndPointQuad.Destination, EndPointQuad.Source,
                options: ReadOnlySpan<byte>.Empty,
                payload: ReadOnlySpan<byte>.Empty);

            var tcp = finPacket.ExtractTcp();
            tcp.SequenceNumber = _sndNxt;
            tcp.AcknowledgmentNumber = _rcvNxt;
            tcp.Finish = true;
            tcp.Acknowledgment = true;
            tcp.WindowSize = LoopbackWindowSize;

            _sndNxt += 1; // FIN consumes one sequence number

            closeAfter = _finReceived;
            State = closeAfter ? TcpConnectionState.Closed : TcpConnectionState.FinWait1;
        }

        stack.SendPacket(finPacket);

        if (closeAfter)
            Close();
    }

    public void TryStartFin(LocalTcpStack stack)
    {
        try { StartFin(stack); } catch { /* ignore */ }
    }

    private void CompleteNetToApp()
    {
        if (_netToAppCompleted) return;
        _netToAppCompleted = true;

        // Use synchronous Complete() intentionally: this is an in-memory Pipe, not a
        // socket/file-backed writer, so completion does not perform async I/O. These
        // close paths are synchronous and may run from packet/state transition code.
        try { _netToAppPipe.Writer.Complete(); } catch { /* already completed */ }
    }

    private void CompleteAppToNet()
    {
        if (_appToNetCompleted) return;
        _appToNetCompleted = true;

        // Use synchronous Complete() intentionally: this is an in-memory Pipe, not a
        // socket/file-backed writer, so completion does not perform async I/O. These
        // close paths are synchronous and may run from packet/state transition code.
        try { _appToNetPipe.Writer.Complete(); } catch { /* already completed */ }
    }

    private void Close()
    {
        if (Interlocked.Exchange(ref _closedFlag, 1) != 0)
            return;

        // Suppress any future FIN emission (RST/idle/double-FIN paths should not send FIN).
        LocalTcpStream? abandoned;
        lock (_seqLock)
        {
            State = TcpConnectionState.Closed;
            _finSent = true;
            abandoned = _pendingStream;
            _pendingStream = null;
        }

        // Dispose unaccepted stream if handshake never completed. With _finSent=true above,
        // the stream's Dispose -> StartFin path is a no-op.
        abandoned?.Dispose();

        CompleteNetToApp();
        CompleteAppToNet();

        try { OnClosed?.Invoke(this); } catch { /* ignore subscriber errors */ }
        Dispose();
    }
}
