using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;

namespace VpnHood.Core.TcpStack;

public sealed class LocalTcpStack
{
    private readonly ConcurrentDictionary<Quad, LocalTcpConnection> _connections = new();
    private readonly ConcurrentDictionary<IPEndPoint, LocalTcpListener> _listeners = new();
    
    public Action<IpPacket>? OnPacketSend { get; set; }

    public LocalTcpListener Listen(IPEndPoint localEndPoint)
    {
        return _listeners.GetOrAdd(localEndPoint, ep => new LocalTcpListener(this, ep, _connections));
    }

    public void ProcessIncoming(ReadOnlySpan<byte> packetData)
    {
        try
        {
            var ipPacket = PacketBuilder.Parse(packetData);
            if (ipPacket.Protocol != IpProtocol.Tcp) return;
            
            var tcpPacket = ipPacket.ExtractTcp();
            var srcEndPoint = new IPEndPoint(ipPacket.SourceAddress, tcpPacket.SourcePort);
            var dstEndPoint = new IPEndPoint(ipPacket.DestinationAddress, tcpPacket.DestinationPort);
            var quad = new Quad(srcEndPoint, dstEndPoint);
            
            if (tcpPacket.Synchronize && !tcpPacket.Acknowledgment)
            {
                // SYN packet - create new connection
                if (!_listeners.TryGetValue(dstEndPoint, out var listener)) return;
                
                var isnLocal = TcpUtil.NewIsn();
                var conn = new LocalTcpConnection(quad, isnLocal, tcpPacket.SequenceNumber);
                
                if (_connections.TryAdd(quad, conn))
                {
                    listener.EnqueueAccept(conn);
                    
                    // Send SYN-ACK
                    var synAckPacket = PacketBuilder.BuildTcp(
                        dstEndPoint, srcEndPoint,
                        ReadOnlySpan<byte>.Empty,
                        ReadOnlySpan<byte>.Empty);
                        
                    var synAckTcp = synAckPacket.ExtractTcp();
                    synAckTcp.SequenceNumber = isnLocal;
                    synAckTcp.AcknowledgmentNumber = tcpPacket.SequenceNumber + 1;
                    synAckTcp.Synchronize = true;
                    synAckTcp.Acknowledgment = true;
                    
                    SendPacket(synAckPacket);
                    conn.SndNxt += 1; // SYN counts as one
                    
                    // Start the connection's data pump
                    _ = Task.Run(() => conn.EmitPendingAsync(this, CancellationToken.None));
                }
                return;
            }
            
            if (_connections.TryGetValue(quad, out var existing))
            {
                var flags = (TcpFlags)0;
                if (tcpPacket.Finish) flags |= TcpFlags.Fin;
                if (tcpPacket.Reset) flags |= TcpFlags.Rst;
                if (tcpPacket.Acknowledgment) flags |= TcpFlags.Ack;
                
                if (existing.TryHandleIncoming(tcpPacket.SequenceNumber, tcpPacket.AcknowledgmentNumber, flags, tcpPacket.Payload.Span, this))
                {
                    // Send ACK back if needed
                    if (tcpPacket.Acknowledgment && tcpPacket.Payload.Length == 0 && !tcpPacket.Finish) return;
                    
                    var ackPacket = PacketBuilder.BuildTcp(
                        dstEndPoint, srcEndPoint,
                        ReadOnlySpan<byte>.Empty,
                        ReadOnlySpan<byte>.Empty);
                        
                    var ackTcp = ackPacket.ExtractTcp();
                    ackTcp.SequenceNumber = existing.SndNxt;
                    ackTcp.AcknowledgmentNumber = existing.RcvNxt;
                    ackTcp.Acknowledgment = true;
                    
                    SendPacket(ackPacket);
                }
            }
        }
        catch
        {
            // Ignore malformed packets
        }
    }

    internal void SendPacket(IpPacket packet)
    {
        packet.UpdateAllChecksums();
        OnPacketSend?.Invoke(packet);
    }
}
