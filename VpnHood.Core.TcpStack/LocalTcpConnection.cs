using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.IO.Pipelines;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;
using VpnHood.Core.TcpStack.Primitives;
using VpnHood.Core.Toolkit.Logging;
using VpnHood.Core.Toolkit.Utils;

namespace VpnHood.Core.TcpStack;

internal sealed class LocalTcpConnection(
    IpEndPointQuad endPointQuad, uint isnLocal, uint isnRemote, TimeSpan? tcpTimeout = null) : IDisposable
{
    // For loopback, we use a moderate fixed window size.
    // The pipe's internal backpressure handles flow control.
    // Large windows waste memory since transfers are instant in loopback.
    private const ushort LoopbackWindowSize = 16384;

    // Idle timeout for connections without activity (safety net for loopback)
    private TimeSpan IdleTimeout { get; } = tcpTimeout ?? TimeSpan.FromMinutes(15);
    private static readonly TimeSpan IdleCheckInterval = TimeSpan.FromMinutes(1);

    // Pipe options - moderate buffer for loopback (no need for large buffers)
    private static readonly PipeOptions PipeOpts = new(
        pauseWriterThreshold: LoopbackWindowSize,
        resumeWriterThreshold: LoopbackWindowSize / 2,
        useSynchronizationContext: false);

    // Pipe for network -> app data (stream reads)
    private readonly Pipe _netToAppPipe = new(PipeOpts);

    // Pipe for app -> network data (stream writes)
    private readonly Pipe _appToNetPipe = new(PipeOpts);

    private readonly Lock _lock = new();
    private readonly CancellationTokenSource _cts = new();
    private bool _finSent;
    private bool _finReceived;
    private bool _disposed;
    private int _closedFlag;
    private long _lastActivityTicks = Stopwatch.GetTimestamp();
    private bool _netToAppCompleted;
    private bool _appToNetCompleted;

    public IpEndPointQuad EndPointQuad { get; } = endPointQuad;
    internal uint SndNxt { get; set; } = isnLocal;
    public uint RcvNxt { get; private set; } = isnRemote + 1; // expecting after SYN
    public TcpConnectionState State { get; internal set; } = TcpConnectionState.SynReceived;

    /// <summary>
    /// Event raised when connection is fully closed and should be removed from the stack.
    /// </summary>
    public event Action<LocalTcpConnection>? OnClosed;

    /// <summary>
    /// PipeReader for reading data received from network (used by LocalTcpStream)
    /// </summary>
    public PipeReader NetToAppReader => _netToAppPipe.Reader;

    /// <summary>
    /// Starts background tasks for this connection. Call after adding to connection dictionary.
    /// </summary>
    public void Start(LocalTcpStack stack)
    {
        // Start idle monitor
        _ = Task.Run(MonitorIdleAsync);

        // Start the connection's data pump
        _ = Task.Run(() => EmitPendingAsync(stack));
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cts.TryCancel();
        _cts.Dispose();
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
        catch (OperationCanceledException) when(_disposed)
        {
            // Expected on close
        }
    }

    /// <summary>
    /// Handles incoming TCP data from network.
    /// Returns: (handled, needsAck) - handled indicates if packet was processed, needsAck if ACK should be sent
    /// </summary>
    public (bool handled, bool needsAck) TryHandleIncoming(uint seq, uint ack, TcpFlags flags, ReadOnlySpan<byte> payload)
    {
        if (_disposed) return (false, false);
        Touch();

        if (flags.HasFlag(TcpFlags.Rst))
        {
            Close();
            return (false, false);
        }

        try
        {
            lock (_lock) // Protect RcvNxt modifications
            {
                // Handle sequence number scenarios
                var seqDiff = (long)seq - RcvNxt;

                if (seqDiff < 0)
                {
                    // Retransmission - ACK it but don't duplicate data
                    // Check for partial overlap with new data
                    var retransmitEnd = seq + (uint)payload.Length;
                    if (retransmitEnd > RcvNxt && payload.Length > 0)
                    {
                        var overlap = (int)(RcvNxt - seq);
                        var newData = payload[overlap..];
                        if (newData.Length > 0)
                        {
                            WriteToAppPipe(newData);
                            RcvNxt += (uint)newData.Length;
                        }
                    }
                    return (true, true); // ACK retransmissions
                }

                if (seqDiff > 0)
                {
                    // Out of order - send duplicate ACK to trigger fast retransmit
                    return (true, true);
                }

                // seq == RcvNxt - expected packet
                if (payload.Length > 0)
                {
                    WriteToAppPipe(payload);
                    RcvNxt += (uint)payload.Length;
                }

                if (flags.HasFlag(TcpFlags.Fin))
                {
                    RcvNxt += 1;
                    _finReceived = true;
                    CompleteNetToApp();

                    // Check if both sides have sent FIN
                    if (_finSent)
                    {
                        State = TcpConnectionState.Closed;
                        Close();
                    }
                    else
                    {
                        State = TcpConnectionState.Closing;
                    }
                    return (true, true); // ACK the FIN
                }
            }

            return (true, payload.Length > 0);
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

        // Fire-and-forget flush - discarding ValueTask has no allocation overhead
        // If flush fails, reader will get exception which is appropriate
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
                if (elapsed >= IdleTimeout)
                {
                    Close();
                    break;
                }
            }
        }
        catch (OperationCanceledException) when(_disposed)
        {
            // Expected on disposal/close
        }
    }

    /// <summary>
    /// Reads from app pipe and emits TCP packets to the network
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

                // Process all available data
                foreach (var segment in buffer)
                {
                    if (segment.IsEmpty)
                        continue;

                    var tcpPacket = PacketBuilder.BuildTcp(
                        EndPointQuad.Destination, EndPointQuad.Source,
                        ReadOnlySpan<byte>.Empty,
                        segment.Span);

                    var tcp = tcpPacket.ExtractTcp();
                    tcp.SequenceNumber = SndNxt;
                    tcp.AcknowledgmentNumber = RcvNxt;
                    tcp.Acknowledgment = true;
                    tcp.Push = true;
                    tcp.WindowSize = LoopbackWindowSize;

                    SndNxt += (uint)segment.Length;
                    stack.SendPacket(tcpPacket);
                }

                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted) break;
            }
        }
        catch (OperationCanceledException) when(_disposed)
        {
            // Expected on close
        }
        catch (Exception)
        {
            // Log exception if needed
            VhLogger.Instance.LogError("Exception in EmitPendingAsync for connection {0}", EndPointQuad);
        }
        finally
        {
            await reader.CompleteAsync();
        }
    }

    public void StartFin(LocalTcpStack stack)
    {
        lock (_lock)
        {
            if (_finSent) return;
            _finSent = true;

            CompleteAppToNet();

            var tcpPacket = PacketBuilder.BuildTcp(
                EndPointQuad.Destination, EndPointQuad.Source,
                ReadOnlySpan<byte>.Empty,
                ReadOnlySpan<byte>.Empty);

            var tcp = tcpPacket.ExtractTcp();
            tcp.SequenceNumber = SndNxt;
            tcp.AcknowledgmentNumber = RcvNxt;
            tcp.Finish = true;
            tcp.Acknowledgment = true;
            tcp.WindowSize = LoopbackWindowSize;

            SndNxt += 1;
            stack.SendPacket(tcpPacket);

            // If we already received FIN from other side, connection is fully closed
            if (_finReceived)
            {
                State = TcpConnectionState.Closed;
                Close();
            }
            else
            {
                State = TcpConnectionState.FinWait1;
            }
        }
    }

    private void CompleteNetToApp()
    {
        if (_netToAppCompleted) return;
        _netToAppCompleted = true;
        _netToAppPipe.Writer.Complete();
    }

    private void CompleteAppToNet()
    {
        if (_appToNetCompleted) return;
        _appToNetCompleted = true;
        _appToNetPipe.Writer.Complete();
    }

    private void Close()
    {
        if (Interlocked.Exchange(ref _closedFlag, 1) != 0)
            return;

        State = TcpConnectionState.Closed;
        CompleteNetToApp();
        CompleteAppToNet();
        OnClosed?.Invoke(this);
        Dispose();
    }
}
