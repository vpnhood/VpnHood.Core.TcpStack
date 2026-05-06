using System.Diagnostics;
using System.IO.Pipelines;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;
using VpnHood.Core.TcpStack.Primitives;

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

    // Pipe options - for network -> app data only (stream reads).
    private static readonly PipeOptions PipeOpts = new(
        pauseWriterThreshold: LoopbackWindowSize,
        resumeWriterThreshold: LoopbackWindowSize / 2,
        useSynchronizationContext: false);

    // Pipe for network -> app data (stream reads)
    private readonly Pipe _netToAppPipe = new(PipeOpts);

    private readonly Lock _seqLock = new();
    private readonly CancellationTokenSource _cts = new();
    private LocalTcpClient? _pendingClient;
    private LocalTcpStack? _stack;

    private bool _finSent;
    private bool _finReceived;
    private bool _sndNxtAfterSynSet;
    private bool _disposed;
    private int _closedFlag;
    private long _lastActivityTicks = Stopwatch.GetTimestamp();
    private bool _netToAppCompleted;
    private bool _appToNetCompleted;
    private uint _sndNxt = isnLocal; // SYN sequence; bumped to ISN+1 after SYN-ACK is sent.
    private uint _rcvNxt = isnRemote + 1; // We have already "consumed" the peer's SYN.

    public IpEndPointQuad EndPointQuad => endPointQuad;
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
        _stack = stack;

        // Pre-create the client (and its stream) that will be enqueued once handshake completes.
        var stream = new LocalTcpStream(this, stack);
        _pendingClient = new LocalTcpClient(
            stream,
            endPointQuad.Destination.ToIPEndPoint(),
            endPointQuad.Source.ToIPEndPoint());

        // Start idle monitor
        _ = Task.Run(MonitorIdleAsync);
    }

    /// <summary>
    /// Gracefully closes the connection: marks the app→net direction as complete, then sends FIN.
    /// Used by <see cref="LocalTcpStream.DisposeAsync"/> so closing the stream does not truncate data.
    /// </summary>
    public Task GracefulCloseAsync(LocalTcpStack stack)
    {
        if (_disposed) return Task.CompletedTask;
        _appToNetCompleted = true;
        TryStartFin(stack);
        return Task.CompletedTask;
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
        LocalTcpClient? clientToEnqueue;
        lock (_seqLock)
        {
            if (State != TcpConnectionState.SynReceived) return;
            State = TcpConnectionState.Established;
            clientToEnqueue = _pendingClient;
            _pendingClient = null;
        }

        if (clientToEnqueue != null && !listener.TryEnqueueAccept(clientToEnqueue))
        {
            // Listener has been stopped: dispose client and reset the connection
            clientToEnqueue.Dispose();
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
    /// Writes data from app directly as TCP segments to the network (inline on caller's thread).
    /// This eliminates cross-thread scheduling latency that was causing slow downloads on Android.
    /// </summary>
    public ValueTask SendAppDataAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        if (_disposed || _appToNetCompleted) return ValueTask.CompletedTask;
        Touch();

        var stack = _stack;
        if (stack == null) return ValueTask.CompletedTask;

        var mss = Mss;
        var span = data.Span;
        var offset = 0;

        while (offset < span.Length)
        {
            ct.ThrowIfCancellationRequested();

            var segLen = Math.Min(span.Length - offset, mss);
            var segmentData = span.Slice(offset, segLen);

            var packet = PacketBuilder.BuildTcp(EndPointQuad.Destination, EndPointQuad.Source,
                options: ReadOnlySpan<byte>.Empty, payload: segmentData);
            var tcp = packet.ExtractTcp();

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

            // Set PSH on the last segment of the current write.
            if (offset + segLen >= span.Length)
                tcp.Push = true;

            stack.SendPacket(packet);
            offset += segLen;
        }

        return ValueTask.CompletedTask;
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

    public void StartFin(LocalTcpStack stack)
    {
        IpPacket? finPacket;
        bool closeAfter;

        lock (_seqLock)
        {
            if (_finSent) return;
            _finSent = true;

            _appToNetCompleted = true;

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

        try { _netToAppPipe.Writer.Complete(); } catch { /* already completed */ }
    }

    private void Close()
    {
        if (Interlocked.Exchange(ref _closedFlag, 1) != 0)
            return;

        LocalTcpClient? abandoned;
        lock (_seqLock)
        {
            State = TcpConnectionState.Closed;
            _finSent = true;
            abandoned = _pendingClient;
            _pendingClient = null;
        }

        // Dispose unaccepted client if handshake never completed.
        abandoned?.Dispose();

        CompleteNetToApp();
        _appToNetCompleted = true;

        try { OnClosed?.Invoke(this); } catch { /* ignore subscriber errors */ }
        Dispose();
    }
}
