using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;
using VpnHood.Core.TcpStack.Primitives;

namespace VpnHood.Core.TcpStack;

/// <summary>
/// A lightweight, localhost-only TCP stack implementation designed for VpnHood's TunVpnAdapter.
/// This stack is optimized for local loopback connections where packet loss is not expected.
/// </summary>
public sealed class LocalTcpStack
{
    // Fixed window size for loopback - no need for large windows since transfer is instant
    private const ushort LoopbackWindowSize = 16384;
    
    private readonly ConcurrentDictionary<Quad, LocalTcpConnection> _connections = new();
    private readonly ConcurrentDictionary<IPEndPoint, LocalTcpListener> _listeners = new();

    /// <summary>
    /// Callback invoked when a TCP packet needs to be sent out.
    /// </summary>
    public Action<IpPacket>? OnPacketSend { get; set; }

    /// <summary>
    /// Creates a TCP listener on the specified local endpoint.
    /// </summary>
    public LocalTcpListener Listen(IPEndPoint localEndPoint)
    {
        return _listeners.GetOrAdd(localEndPoint, ep => new LocalTcpListener(this, ep));
    }

    /// <summary>
    /// Stops listening on the specified endpoint and removes the listener.
    /// </summary>
    public bool StopListening(IPEndPoint localEndPoint)
    {
        return _listeners.TryRemove(localEndPoint, out _);
    }

    /// <summary>
    /// Processes an incoming IP packet that may contain TCP data.
    /// </summary>
    public void ProcessIncoming(ReadOnlySpan<byte> packetData)
    {
        try
        {
            var ipPacket = PacketBuilder.Parse(packetData);

            if (ipPacket.Protocol != IpProtocol.Tcp)
                return;

            var tcpPacket = ipPacket.ExtractTcp();
            var srcEndPoint = new IPEndPoint(ipPacket.SourceAddress, tcpPacket.SourcePort);
            var dstEndPoint = new IPEndPoint(ipPacket.DestinationAddress, tcpPacket.DestinationPort);
            var quad = new Quad(srcEndPoint, dstEndPoint);

            // Handle SYN packets (new connection requests)
            if (tcpPacket is { Synchronize: true, Acknowledgment: false })
            {
                HandleSynPacket(quad, dstEndPoint, srcEndPoint, tcpPacket);
                return;
            }

            // Handle packets for existing connections
            if (_connections.TryGetValue(quad, out var existing))
            {
                HandleExistingConnection(existing, tcpPacket, dstEndPoint, srcEndPoint);
                return;
            }
            
            // Unknown connection - send RST if not already a RST
            if (!tcpPacket.Reset)
            {
                SendRst(dstEndPoint, srcEndPoint, tcpPacket);
            }
        }
        catch
        {
            // Silently ignore malformed packets
        }
    }

    private void HandleSynPacket(Quad quad, IPEndPoint dstEndPoint, IPEndPoint srcEndPoint, TcpPacket tcpPacket)
    {
        if (!_listeners.TryGetValue(dstEndPoint, out var listener))
        {
            // No listener - send RST
            SendRst(dstEndPoint, srcEndPoint, tcpPacket);
            return;
        }

        var isnLocal = (uint)RandomNumberGenerator.GetInt32(int.MaxValue);
        var conn = new LocalTcpConnection(quad, isnLocal, tcpPacket.SequenceNumber);
        
        // Subscribe to connection close event for cleanup
        conn.OnClosed += OnConnectionClosed;

        if (!_connections.TryAdd(quad, conn))
            return;

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
        synAckTcp.WindowSize = LoopbackWindowSize;

        SendPacket(synAckPacket);
        conn.SndNxt += 1; // SYN counts as one sequence number

        // Start the connection's data pump
        _ = Task.Run(() => conn.EmitPendingAsync(this, CancellationToken.None));
    }

    private void HandleExistingConnection(LocalTcpConnection conn, TcpPacket tcpPacket, IPEndPoint dstEndPoint, IPEndPoint srcEndPoint)
    {
        // Transition from SynReceived to Established on first ACK
        if (conn.State == TcpConnectionState.SynReceived && tcpPacket.Acknowledgment)
            conn.State = TcpConnectionState.Established;

        var flags = (TcpFlags)0;
        if (tcpPacket.Finish) flags |= TcpFlags.Fin;
        if (tcpPacket.Reset) flags |= TcpFlags.Rst;
        if (tcpPacket.Acknowledgment) flags |= TcpFlags.Ack;

        var (handled, needsAck) = conn.TryHandleIncoming(
            tcpPacket.SequenceNumber, 
            tcpPacket.AcknowledgmentNumber, 
            flags, 
            tcpPacket.Payload.Span);
        
        if (!handled)
            return;

        if (!needsAck)
            return;

        var ackPacket = PacketBuilder.BuildTcp(
            dstEndPoint, srcEndPoint,
            ReadOnlySpan<byte>.Empty,
            ReadOnlySpan<byte>.Empty);

        var ackTcp = ackPacket.ExtractTcp();
        ackTcp.SequenceNumber = conn.SndNxt;
        ackTcp.AcknowledgmentNumber = conn.RcvNxt;
        ackTcp.Acknowledgment = true;
        ackTcp.WindowSize = LoopbackWindowSize;

        SendPacket(ackPacket);
    }

    private void SendRst(IPEndPoint localEndPoint, IPEndPoint remoteEndPoint, TcpPacket incomingTcp)
    {
        var rstPacket = PacketBuilder.BuildTcp(
            localEndPoint, remoteEndPoint,
            ReadOnlySpan<byte>.Empty,
            ReadOnlySpan<byte>.Empty);

        var rstTcp = rstPacket.ExtractTcp();
        rstTcp.Reset = true;
        
        // RFC 793: If ACK bit is off, seq = 0, ack = seq + segment length
        // If ACK bit is on, seq = ack number from incoming
        if (incomingTcp.Acknowledgment)
        {
            rstTcp.SequenceNumber = incomingTcp.AcknowledgmentNumber;
        }
        else
        {
            rstTcp.SequenceNumber = 0;
            rstTcp.AcknowledgmentNumber = incomingTcp.SequenceNumber + (uint)incomingTcp.Payload.Length;
            if (incomingTcp.Synchronize) rstTcp.AcknowledgmentNumber += 1;
            if (incomingTcp.Finish) rstTcp.AcknowledgmentNumber += 1;
            rstTcp.Acknowledgment = true;
        }

        SendPacket(rstPacket);
    }

    private void OnConnectionClosed(LocalTcpConnection conn)
    {
        _connections.TryRemove(conn.Quad, out _);
    }

    internal void SendPacket(IpPacket packet)
    {
        packet.UpdateAllChecksums();
        OnPacketSend?.Invoke(packet);
    }
}
