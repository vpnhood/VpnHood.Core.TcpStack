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
        Console.WriteLine($"[TCP STACK] Listening on {localEndPoint}");
        return _listeners.GetOrAdd(localEndPoint, ep => new LocalTcpListener(this, ep, _connections));
    }

    public void ProcessIncoming(ReadOnlySpan<byte> packetData)
    {
        try
        {
            var ipPacket = PacketBuilder.Parse(packetData);
            Console.WriteLine($"[TCP STACK] Processing packet: Proto={ipPacket.Protocol}, Src={ipPacket.SourceAddress}, Dst={ipPacket.DestinationAddress}");
            
            if (ipPacket.Protocol != IpProtocol.Tcp)
            {
                Console.WriteLine("[TCP STACK] Not TCP, ignoring");
                return;
            }
            
            var tcpPacket = ipPacket.ExtractTcp();
            var srcEndPoint = new IPEndPoint(ipPacket.SourceAddress, tcpPacket.SourcePort);
            var dstEndPoint = new IPEndPoint(ipPacket.DestinationAddress, tcpPacket.DestinationPort);
            var quad = new Quad(srcEndPoint, dstEndPoint);
            
            Console.WriteLine($"[TCP STACK] TCP Packet: {srcEndPoint} -> {dstEndPoint}, Flags: SYN={tcpPacket.Synchronize}, ACK={tcpPacket.Acknowledgment}, FIN={tcpPacket.Finish}, Seq={tcpPacket.SequenceNumber}");
            Console.WriteLine($"[TCP STACK] Active listeners: {string.Join(", ", _listeners.Keys)}");
            
            if (tcpPacket.Synchronize && !tcpPacket.Acknowledgment)
            {
                Console.WriteLine($"[TCP STACK] SYN packet detected, looking for listener on {dstEndPoint}");
                
                // SYN packet - create new connection
                if (!_listeners.TryGetValue(dstEndPoint, out var listener))
                {
                    Console.WriteLine($"[TCP STACK] No listener found for {dstEndPoint}");
                    return;
                }
                
                Console.WriteLine("[TCP STACK] Listener found! Creating connection...");
                var isnLocal = TcpUtil.NewIsn();
                var conn = new LocalTcpConnection(quad, isnLocal, tcpPacket.SequenceNumber);
                
                if (_connections.TryAdd(quad, conn))
                {
                    Console.WriteLine("[TCP STACK] Connection created, enqueueing to listener");
                    listener.EnqueueAccept(conn);
                    
                    // Send SYN-ACK
                    Console.WriteLine($"[TCP STACK] Sending SYN-ACK: {dstEndPoint} -> {srcEndPoint}, Seq={isnLocal}, Ack={tcpPacket.SequenceNumber + 1}");
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
                else
                {
                    Console.WriteLine("[TCP STACK] Failed to add connection (already exists?)");
                }
                return;
            }
            
            if (_connections.TryGetValue(quad, out var existing))
            {
                Console.WriteLine($"[TCP STACK] Found existing connection for {quad}");
                
                var flags = (TcpFlags)0;
                if (tcpPacket.Finish) flags |= TcpFlags.Fin;
                if (tcpPacket.Reset) flags |= TcpFlags.Rst;
                if (tcpPacket.Acknowledgment) flags |= TcpFlags.Ack;
                
                if (existing.TryHandleIncoming(tcpPacket.SequenceNumber, tcpPacket.AcknowledgmentNumber, flags, tcpPacket.Payload.Span, this))
                {
                    // Send ACK back if needed
                    if (tcpPacket.Acknowledgment && tcpPacket.Payload.Length == 0 && !tcpPacket.Finish)
                    {
                        Console.WriteLine("[TCP STACK] Pure ACK, not responding");
                        return;
                    }
                    
                    Console.WriteLine($"[TCP STACK] Sending ACK: Seq={existing.SndNxt}, Ack={existing.RcvNxt}");
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
            else
            {
                Console.WriteLine($"[TCP STACK] No existing connection found for {quad}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TCP STACK] ERROR processing packet: {ex.Message}");
            Console.WriteLine($"[TCP STACK] Stack trace: {ex.StackTrace}");
        }
    }

    internal void SendPacket(IpPacket packet)
    {
        Console.WriteLine($"[TCP STACK] Sending packet: {packet.SourceAddress}:{packet.DestinationAddress}");
        packet.UpdateAllChecksums();
        OnPacketSend?.Invoke(packet);
    }
}
