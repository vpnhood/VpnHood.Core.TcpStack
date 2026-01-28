using System.Buffers;
using System.IO.Pipelines;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;
using VpnHood.Core.TcpStack.Primitives;

namespace VpnHood.Core.TcpStack;

internal sealed class LocalTcpConnection(Quad quad, uint isnLocal, uint isnRemote) : IDisposable
{
    private readonly Pipe _netToAppPipe = new(new PipeOptions(useSynchronizationContext: false));
    private readonly Lock _lock = new();
    private bool _finSent;
    private bool _disposed;

    // Pipe for app -> network data (stream writes)
    private readonly Pipe _appToNetPipe = new(new PipeOptions(useSynchronizationContext: false));
    // Pipe for network -> app data (stream reads)

    public Quad Quad { get; } = quad;
    internal uint SndNxt { get; set; } = isnLocal;
    public uint RcvNxt { get; private set; } = isnRemote + 1; // expecting after SYN
    public TcpConnectionState State { get; internal set; } = TcpConnectionState.SynReceived;
    public CancellationToken ConnectionClosed => _cts.Token;
    private readonly CancellationTokenSource _cts = new();
    
    /// <summary>
    /// PipeReader for reading data received from network (used by LocalTcpStream)
    /// </summary>
    public PipeReader NetToAppReader => _netToAppPipe.Reader;

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
    /// Handles incoming TCP data from network - writes directly to pipe without allocation
    /// </summary>
    public bool TryHandleIncoming(uint seq, uint ack, TcpFlags flags, ReadOnlySpan<byte> payload, LocalTcpStack stack)
    {
        if (flags.HasFlag(TcpFlags.Rst)) { Close(); return false; }
        if (seq != RcvNxt) return false; // out of order (not expected in localhost simplified model)
        
        if (payload.Length > 0)
        {
            RcvNxt += (uint)payload.Length;
            
            // Write directly to pipe - uses pooled memory
            var span = _netToAppPipe.Writer.GetSpan(payload.Length);
            payload.CopyTo(span);
            _netToAppPipe.Writer.Advance(payload.Length);
            
            // Flush synchronously since we're in the packet processing path
            var flushTask = _netToAppPipe.Writer.FlushAsync();
            if (!flushTask.IsCompleted)
                flushTask.AsTask().GetAwaiter().GetResult();
        }
        
        if (flags.HasFlag(TcpFlags.Fin))
        {
            RcvNxt += 1;
            _netToAppPipe.Writer.Complete();
            State = TcpConnectionState.Closing;
        }
        return true;
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
                    tcp.WindowSize = 65535; // Advertise large receive window
                    
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
            tcp.WindowSize = 65535; // Advertise large receive window
            
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
