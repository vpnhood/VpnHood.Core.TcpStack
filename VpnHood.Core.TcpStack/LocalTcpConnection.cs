using System.Buffers;
using System.IO.Pipelines;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;
using VpnHood.Core.TcpStack.Primitives;

namespace VpnHood.Core.TcpStack;

internal sealed class LocalTcpConnection : IDisposable
{
    // Default receive window size
    private const int DefaultWindowSize = 65535;
    
    // Pipe options with reasonable buffer sizes for flow control
    private static readonly PipeOptions NetToAppPipeOptions = new(
        pauseWriterThreshold: DefaultWindowSize,
        resumeWriterThreshold: DefaultWindowSize / 2,
        useSynchronizationContext: false);
    
    private static readonly PipeOptions AppToNetPipeOptions = new(
        useSynchronizationContext: false);

    // Pipe for network -> app data (stream reads)
    private readonly Pipe _netToAppPipe = new(NetToAppPipeOptions);
    
    // Pipe for app -> network data (stream writes)
    private readonly Pipe _appToNetPipe = new(AppToNetPipeOptions);
    
    private readonly Lock _lock = new();
    private bool _finSent;
    private bool _disposed;
    private long _bytesBuffered; // Track how many bytes are buffered but not yet read by app

    public Quad Quad { get; }
    internal uint SndNxt { get; set; }
    public uint RcvNxt { get; private set; }
    public TcpConnectionState State { get; internal set; } = TcpConnectionState.SynReceived;
    public CancellationToken ConnectionClosed => _cts.Token;
    private readonly CancellationTokenSource _cts = new();
    
    /// <summary>
    /// PipeReader for reading data received from network (used by LocalTcpStream)
    /// </summary>
    public PipeReader NetToAppReader => _netToAppPipe.Reader;
    
    /// <summary>
    /// Gets the current advertised window size based on buffer availability
    /// </summary>
    public ushort CurrentWindowSize
    {
        get
        {
            var buffered = Interlocked.Read(ref _bytesBuffered);
            var available = DefaultWindowSize - buffered;
            return (ushort)Math.Clamp(available, 0, ushort.MaxValue);
        }
    }

    public LocalTcpConnection(Quad quad, uint isnLocal, uint isnRemote)
    {
        Quad = quad;
        SndNxt = isnLocal;
        RcvNxt = isnRemote + 1; // expecting after SYN
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        
        if (!_cts.IsCancellationRequested)
            _cts.Cancel();
        _cts.Dispose();
    }

    /// <summary>
    /// Writes data from app to the network pipe (zero-copy when possible)
    /// </summary>
    public async ValueTask SendAppDataAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        var result = await _appToNetPipe.Writer.WriteAsync(data, ct);
        if (result.IsCompleted || result.IsCanceled)
            return;
    }

    /// <summary>
    /// Called when app reads data from the stream - updates window tracking
    /// </summary>
    internal void OnDataConsumed(int bytesConsumed)
    {
        Interlocked.Add(ref _bytesBuffered, -bytesConsumed);
    }

    /// <summary>
    /// Handles incoming TCP data from network.
    /// Returns: (handled, needsAck) - handled indicates if packet was processed, needsAck if ACK should be sent
    /// </summary>
    public (bool handled, bool needsAck) TryHandleIncoming(uint seq, uint ack, TcpFlags flags, ReadOnlySpan<byte> payload, LocalTcpStack stack)
    {
        if (flags.HasFlag(TcpFlags.Rst)) 
        { 
            Close(); 
            return (false, false); 
        }

        // Handle sequence number scenarios
        var seqDiff = (long)seq - RcvNxt;
        
        if (seqDiff < 0)
        {
            // This is a retransmission of data we've already received
            // We need to ACK it so the sender knows we got it, but don't process the data again
            
            // Check if this retransmission overlaps with data we haven't received yet
            var retransmitEnd = seq + (uint)payload.Length;
            if (retransmitEnd > RcvNxt && payload.Length > 0)
            {
                // Partial overlap - extract only the new portion
                var overlap = (int)(RcvNxt - seq);
                var newData = payload.Slice(overlap);
                if (newData.Length > 0)
                {
                    WriteToAppPipe(newData);
                    RcvNxt += (uint)newData.Length;
                }
            }
            
            // Always ACK retransmissions so sender stops retrying
            return (true, true);
        }
        
        if (seqDiff > 0)
        {
            // Out of order packet - we're missing data
            // In loopback this shouldn't happen often, but if it does, 
            // we need to send a duplicate ACK to trigger fast retransmit
            return (true, true);
        }
        
        // seq == RcvNxt - this is the expected packet
        if (payload.Length > 0)
        {
            WriteToAppPipe(payload);
            RcvNxt += (uint)payload.Length;
        }
        
        if (flags.HasFlag(TcpFlags.Fin))
        {
            RcvNxt += 1;
            _netToAppPipe.Writer.Complete();
            State = TcpConnectionState.Closing;
            return (true, true); // ACK the FIN
        }
        
        // Need ACK if we received data
        return (true, payload.Length > 0);
    }

    private void WriteToAppPipe(ReadOnlySpan<byte> data)
    {
        // Track buffered bytes for window calculation
        Interlocked.Add(ref _bytesBuffered, data.Length);
        
        // Get buffer from pipe and copy data
        var span = _netToAppPipe.Writer.GetSpan(data.Length);
        data.CopyTo(span);
        _netToAppPipe.Writer.Advance(data.Length);
        
        // Flush asynchronously to avoid blocking packet processing
        // The pipe has backpressure built in via pauseWriterThreshold
        var flushTask = _netToAppPipe.Writer.FlushAsync();
        if (!flushTask.IsCompleted)
        {
            // If flush would block, let it complete in background
            // This prevents blocking the packet processing thread
            _ = flushTask.AsTask().ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    // Pipe was completed (connection closed)
                    Close();
                }
            }, TaskContinuationOptions.OnlyOnFaulted);
        }
    }

    /// <summary>
    /// Reads from app pipe and emits TCP packets to the network
    /// </summary>
    public async Task EmitPendingAsync(LocalTcpStack stack, CancellationToken ct)
    {
        var reader = _appToNetPipe.Reader;
        try
        {
            while (State != TcpConnectionState.Closed)
            {
                var result = await reader.ReadAsync(ct);
                var buffer = result.Buffer;
                
                if (buffer.IsEmpty)
                {
                    if (result.IsCompleted)
                        break;
                    reader.AdvanceTo(buffer.End);
                    continue;
                }

                // Process all available data in one go
                foreach (var segment in buffer)
                {
                    if (segment.IsEmpty) continue;
                    
                    // Create TCP packet directly from the segment span
                    var tcpPacket = PacketBuilder.BuildTcp(
                        Quad.Destination, Quad.Source, // reversed because quad stored original SYN direction
                        ReadOnlySpan<byte>.Empty, // no options
                        segment.Span);
                    
                    var tcp = tcpPacket.ExtractTcp();
                    tcp.SequenceNumber = SndNxt;
                    tcp.AcknowledgmentNumber = RcvNxt;
                    tcp.Acknowledgment = true;
                    tcp.Push = true;
                    tcp.WindowSize = CurrentWindowSize;
                    
                    SndNxt += (uint)segment.Length;
                    stack.SendPacket(tcpPacket);
                }
                
                // Mark all data as consumed
                reader.AdvanceTo(buffer.End);
                
                if (_finSent && State == TcpConnectionState.Closing)
                {
                    Close();
                    break;
                }
                
                if (result.IsCompleted)
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when connection is closed
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
            
            // Complete the app->net pipe to signal no more data
            _appToNetPipe.Writer.Complete();
            
            var tcpPacket = PacketBuilder.BuildTcp(
                Quad.Destination, Quad.Source,
                ReadOnlySpan<byte>.Empty,
                ReadOnlySpan<byte>.Empty);
                
            var tcp = tcpPacket.ExtractTcp();
            tcp.SequenceNumber = SndNxt;
            tcp.AcknowledgmentNumber = RcvNxt;
            tcp.Finish = true;
            tcp.Acknowledgment = true;
            tcp.WindowSize = CurrentWindowSize;
            
            SndNxt += 1;
            stack.SendPacket(tcpPacket);
        }
    }

    private void Close()
    {
        State = TcpConnectionState.Closed;
        _netToAppPipe.Writer.Complete();
        _appToNetPipe.Writer.Complete();
        Dispose();
    }
}
