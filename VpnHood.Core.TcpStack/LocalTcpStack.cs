using System.Collections.Concurrent;
using System.Net;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;

namespace VpnHood.Core.TcpStack;

/// <summary>
/// A lightweight, localhost-only TCP stack implementation designed for VpnHood's TunVpnAdapter.
/// This stack is optimized for local connections where packet loss is not expected.
/// </summary>
public sealed class LocalTcpStack
{
    private readonly ConcurrentDictionary<Quad, LocalTcpConnection> _connections = new();
    private readonly ConcurrentDictionary<IPEndPoint, LocalTcpListener> _listeners = new();

    /// <summary>
    /// Callback invoked when a TCP packet needs to be sent out.
    /// </summary>
    public Action<IpPacket>? OnPacketSend { get; set; }

    /// <summary>
    /// Creates a TCP listener on the specified local endpoint.
    /// </summary>
    /// <param name="localEndPoint">The IP endpoint to listen on.</param>
    /// <returns>A <see cref="LocalTcpListener"/> that can accept incoming connections.</returns>
    public LocalTcpListener Listen(IPEndPoint localEndPoint)
    {
        return _listeners.GetOrAdd(localEndPoint, ep => new LocalTcpListener(this, ep, _connections));
    }

    /// <summary>
    /// Stops listening on the specified endpoint and removes the listener.
    /// </summary>
    /// <param name="localEndPoint">The endpoint to stop listening on.</param>
    /// <returns>True if the listener was removed; false if it didn't exist.</returns>
    public bool StopListening(IPEndPoint localEndPoint)
    {
        return _listeners.TryRemove(localEndPoint, out _);
    }

    /// <summary>
    /// Processes an incoming IP packet that may contain TCP data.
    /// </summary>
    /// <param name="packetData">The raw packet data.</param>
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
            if (tcpPacket.Synchronize && !tcpPacket.Acknowledgment)
            {
                HandleSynPacket(quad, dstEndPoint, srcEndPoint, tcpPacket);
                return;
            }

            // Handle packets for existing connections
            if (_connections.TryGetValue(quad, out var existing))
                HandleExistingConnection(existing, tcpPacket, dstEndPoint, srcEndPoint);
        }
        catch
        {
            // Silently ignore malformed packets
        }
    }

    private void HandleSynPacket(Quad quad, IPEndPoint dstEndPoint, IPEndPoint srcEndPoint, TcpPacket tcpPacket)
    {
        if (!_listeners.TryGetValue(dstEndPoint, out var listener))
            return;

        var isnLocal = TcpUtil.NewIsn();
        var conn = new LocalTcpConnection(quad, isnLocal, tcpPacket.SequenceNumber);

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

        SendPacket(synAckPacket);
        conn.SndNxt += 1; // SYN counts as one sequence number

        // Start the connection's data pump
        _ = Task.Run(() => conn.EmitPendingAsync(this, CancellationToken.None));
    }

    private void HandleExistingConnection(LocalTcpConnection conn, TcpPacket tcpPacket, IPEndPoint dstEndPoint, IPEndPoint srcEndPoint)
    {
        // Transition from SynReceived to Established on first ACK
        if (conn.State == TcpConnState.SynReceived && tcpPacket.Acknowledgment)
            conn.State = TcpConnState.Established;

        var flags = (TcpFlags)0;
        if (tcpPacket.Finish) flags |= TcpFlags.Fin;
        if (tcpPacket.Reset) flags |= TcpFlags.Rst;
        if (tcpPacket.Acknowledgment) flags |= TcpFlags.Ack;

        if (!conn.TryHandleIncoming(tcpPacket.SequenceNumber, tcpPacket.AcknowledgmentNumber, flags, tcpPacket.Payload.Span, this))
            return;

        // Don't respond to pure ACKs (no payload, no FIN)
        if (tcpPacket.Acknowledgment && tcpPacket.Payload.Length == 0 && !tcpPacket.Finish)
            return;

        // Send ACK for received data or FIN
        var ackPacket = PacketBuilder.BuildTcp(
            dstEndPoint, srcEndPoint,
            ReadOnlySpan<byte>.Empty,
            ReadOnlySpan<byte>.Empty);

        var ackTcp = ackPacket.ExtractTcp();
        ackTcp.SequenceNumber = conn.SndNxt;
        ackTcp.AcknowledgmentNumber = conn.RcvNxt;
        ackTcp.Acknowledgment = true;

        SendPacket(ackPacket);
    }

    internal void SendPacket(IpPacket packet)
    {
        packet.UpdateAllChecksums();
        OnPacketSend?.Invoke(packet);
    }
}
