using System.Net;
using System.Threading.Channels;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;

namespace VpnHood.Core.TcpStack;

internal enum TcpConnState
{
    SynReceived,
    Established,
    FinWait1,
    Closing,
    Closed
}

internal sealed class LocalTcpConnection : IDisposable
{
    private readonly Channel<byte[]> _appToNet = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true });
    private readonly Channel<byte[]> _netToApp = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleWriter = true });
    private readonly object _lock = new();
    private bool _finSent;
    
    public Quad Quad { get; }
    internal uint SndNxt { get; set; }
    public uint RcvNxt { get; private set; }
    public TcpConnState State { get; private set; }
    public CancellationToken ConnectionClosed => _cts.Token;
    private readonly CancellationTokenSource _cts = new();
    
    public LocalTcpConnection(Quad quad, uint isnLocal, uint isnRemote)
    {
        Quad = quad;
        SndNxt = isnLocal;
        RcvNxt = isnRemote + 1; // expecting after SYN
        State = TcpConnState.SynReceived;
    }

    public void Dispose()
    {
        if (!_cts.IsCancellationRequested)
            _cts.Cancel();
        _cts.Dispose();
    }

    public ValueTask SendAppDataAsync(byte[] data, CancellationToken ct = default) => _appToNet.Writer.WriteAsync(data, ct);
    public IAsyncEnumerable<byte[]> ReadAppDataAsync(CancellationToken ct = default) => _netToApp.Reader.ReadAllAsync(ct);

    public bool TryHandleIncoming(uint seq, uint ack, TcpFlags flags, ReadOnlySpan<byte> payload, LocalTcpStack stack)
    {
        if (flags.HasFlag(TcpFlags.Rst)) { Close(); return false; }
        if (seq != RcvNxt) return false; // out of order (not expected in localhost simplified model)
        
        if (payload.Length > 0)
        {
            RcvNxt += (uint)payload.Length;
            _netToApp.Writer.TryWrite(payload.ToArray());
        }
        
        if (flags.HasFlag(TcpFlags.Fin))
        {
            RcvNxt += 1;
            _netToApp.Writer.Complete();
            State = TcpConnState.Closing;
        }
        return true;
    }

    public async Task EmitPendingAsync(LocalTcpStack stack, CancellationToken ct)
    {
        while (State != TcpConnState.Closed && await _appToNet.Reader.WaitToReadAsync(ct))
        {
            while (_appToNet.Reader.TryRead(out var data))
            {
                if (data.Length == 0) continue;
                
                // Create TCP packet using PacketBuilder
                var tcpPacket = PacketBuilder.BuildTcp(
                    Quad.Destination, Quad.Source, // reversed because quad stored original SYN direction
                    ReadOnlySpan<byte>.Empty, // no options
                    data);
                
                var tcp = tcpPacket.ExtractTcp();
                tcp.SequenceNumber = SndNxt;
                tcp.AcknowledgmentNumber = RcvNxt;
                tcp.Acknowledgment = true;
                tcp.Push = true;
                
                SndNxt += (uint)data.Length;
                stack.SendPacket(tcpPacket);
            }
            
            if (_finSent && State == TcpConnState.Closing)
            {
                Close();
            }
        }
    }

    public void StartFin(LocalTcpStack stack)
    {
        lock (_lock)
        {
            if (_finSent) return;
            _finSent = true;
            
            var tcpPacket = PacketBuilder.BuildTcp(
                Quad.Destination, Quad.Source,
                ReadOnlySpan<byte>.Empty,
                ReadOnlySpan<byte>.Empty);
                
            var tcp = tcpPacket.ExtractTcp();
            tcp.SequenceNumber = SndNxt;
            tcp.AcknowledgmentNumber = RcvNxt;
            tcp.Finish = true;
            tcp.Acknowledgment = true;
            
            SndNxt += 1;
            stack.SendPacket(tcpPacket);
        }
    }

    private void Close()
    {
        State = TcpConnState.Closed;
        _netToApp.Writer.TryComplete();
        _appToNet.Writer.TryComplete();
        Dispose();
    }
}
