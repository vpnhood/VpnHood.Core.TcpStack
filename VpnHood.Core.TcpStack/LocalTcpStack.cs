using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;
using VpnHood.Core.TcpStack.Primitives;
using VpnHood.Core.Toolkit.Net;

namespace VpnHood.Core.TcpStack;

/// <summary>
/// A lightweight, localhost-only TCP stack implementation designed for VpnHood's TunVpnAdapter.
/// This stack is optimized for local loopback connections where packet loss is not expected.
/// </summary>
public sealed class LocalTcpStack : IDisposable
{
    // Fixed window size for loopback - no need for large windows since transfer is instant
    private const ushort LoopbackWindowSize = 16384;

    private readonly ConcurrentDictionary<IpEndPointQuad, LocalTcpConnection> _connections = new();
    private readonly ConcurrentDictionary<IpEndPointValue, LocalTcpListener> _listeners = new();
    private LocalTcpListener? _anyListener;
    private bool _disposed;

    /// <summary>
    /// Callback invoked when a TCP packet needs to be sent out.
    /// </summary>
    public Action<IpPacket>? OnPacketSend { get; set; }

    /// <summary>
    /// Creates a TCP listener on the specified local endpoint.
    /// </summary>
    public LocalTcpListener Listen(IpEndPointValue localEndPoint)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var ipEndPoint = new IpEndPointValue(localEndPoint.Address, localEndPoint.Port);
        return _listeners.GetOrAdd(ipEndPoint, _ => new LocalTcpListener(this, localEndPoint));
    }

    /// <summary>
    /// Creates a TCP listener that accepts connections on any endpoint (IPv4 and IPv6).
    /// </summary>
    public LocalTcpListener ListenAny()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _anyListener ??= new LocalTcpListener(this, null);
    }

    public bool StopListening(IPEndPoint localEndPoint)
    {
        return StopListening(localEndPoint.ToValue());
    }

    /// <summary>
    /// Stops listening on the specified endpoint and removes the listener.
    /// </summary>
    public bool StopListening(IpEndPointValue localEndPoint)
    {
        var endPointStruct = new IpEndPointValue(localEndPoint.Address, localEndPoint.Port);
        return _listeners.TryRemove(endPointStruct, out _);
    }

    /// <summary>
    /// Stops the wildcard listener that accepts connections on any endpoint.
    /// </summary>
    internal bool StopListeningAny()
    {
        if (_anyListener == null)
            return false;

        _anyListener = null;
        return true;
    }

    /// <summary>
    /// Processes an incoming IP packet that may contain TCP data.
    /// </summary>
    public void ProcessIncoming(ReadOnlySpan<byte> packetData)
    {
        if (_disposed)
            return;

        try
        {
            var ipPacket = PacketBuilder.Parse(packetData);
            if (ipPacket.Protocol != IpProtocol.Tcp)
                return;

            var tcpPacket = ipPacket.ExtractTcp();
            var ipEndPointQuad = new IpEndPointQuad(
                new IpEndPointValue(ipPacket.SourceAddress, tcpPacket.SourcePort),
                new IpEndPointValue(ipPacket.DestinationAddress, tcpPacket.DestinationPort));

            // Handle SYN packets (new connection requests)
            if (tcpPacket is { Synchronize: true, Acknowledgment: false })
            {
                HandleSynPacket(ipEndPointQuad, tcpPacket);
                return;
            }

            // Handle packets for existing connections
            if (_connections.TryGetValue(ipEndPointQuad, out var existing))
            {
                HandleExistingConnection(existing, tcpPacket);
                return;
            }

            // Unknown connection - send RST if not already a RST
            if (!tcpPacket.Reset)
            {
                SendRst(ipEndPointQuad.Destination, ipEndPointQuad.Source, tcpPacket);
            }
        }
        catch
        {
            // Silently ignore malformed packets
        }
    }

    private void HandleSynPacket(IpEndPointQuad ipEndPointQuad, TcpPacket tcpPacket)
    {
        var listener = ResolveListener(ipEndPointQuad.Destination);
        if (listener == null)
        {
            // No listener - send RST
            SendRst(ipEndPointQuad.Destination, ipEndPointQuad.Source, tcpPacket);
            return;
        }

        // Check if connection already exists (SYN retransmit)
        if (_connections.TryGetValue(ipEndPointQuad, out var existingConn))
        {
            // SYN retransmit - resend SYN-ACK
            if (existingConn.State == TcpConnectionState.SynReceived)
            {
                var synAckPacket = PacketBuilder.BuildTcp(ipEndPointQuad.Destination, ipEndPointQuad.Source,
                    ReadOnlySpan<byte>.Empty,
                    ReadOnlySpan<byte>.Empty);

                var synAckTcp = synAckPacket.ExtractTcp();
                synAckTcp.SequenceNumber = existingConn.SndNxt - 1; // SYN seq was SndNxt - 1
                synAckTcp.AcknowledgmentNumber = existingConn.RcvNxt;
                synAckTcp.Synchronize = true;
                synAckTcp.Acknowledgment = true;
                synAckTcp.WindowSize = LoopbackWindowSize;

                SendPacket(synAckPacket);
            }
            return;
        }

        var isnLocal = (uint)RandomNumberGenerator.GetInt32(int.MaxValue);
        var tcpConnection = new LocalTcpConnection(ipEndPointQuad, isnLocal, tcpPacket.SequenceNumber);

        // Subscribe to connection close event for cleanup
        tcpConnection.OnClosed += OnConnectionClosed;

        if (!_connections.TryAdd(ipEndPointQuad, tcpConnection))
        {
            tcpConnection.Dispose();
            return;
        }

        listener.EnqueueAccept(tcpConnection);

        // Send SYN-ACK
        var synAckPacket2 = PacketBuilder.BuildTcp(
            ipEndPointQuad.Destination, ipEndPointQuad.Source,
            ReadOnlySpan<byte>.Empty,
            ReadOnlySpan<byte>.Empty);

        var synAckTcp2 = synAckPacket2.ExtractTcp();
        synAckTcp2.SequenceNumber = isnLocal;
        synAckTcp2.AcknowledgmentNumber = tcpPacket.SequenceNumber + 1;
        synAckTcp2.Synchronize = true;
        synAckTcp2.Acknowledgment = true;
        synAckTcp2.WindowSize = LoopbackWindowSize;

        SendPacket(synAckPacket2);
        tcpConnection.SndNxt += 1; // SYN counts as one sequence number

        // Start background tasks (idle monitor, data pump)
        tcpConnection.Start(this);
    }

    private void HandleExistingConnection(LocalTcpConnection conn, TcpPacket tcpPacket)
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
            conn.EndPointQuad.Destination, conn.EndPointQuad.Source,
            ReadOnlySpan<byte>.Empty,
            ReadOnlySpan<byte>.Empty);

        var ackTcp = ackPacket.ExtractTcp();
        ackTcp.SequenceNumber = conn.SndNxt;
        ackTcp.AcknowledgmentNumber = conn.RcvNxt;
        ackTcp.Acknowledgment = true;
        ackTcp.WindowSize = LoopbackWindowSize;

        SendPacket(ackPacket);
    }

    private void SendRst(IpEndPointValue localEndPoint, IpEndPointValue remoteEndPoint, TcpPacket incomingTcp)
    {
        var rstPacket = PacketBuilder.BuildTcp(
            localEndPoint.ToIPEndPoint(), remoteEndPoint.ToIPEndPoint(),
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
        _connections.TryRemove(conn.EndPointQuad, out _);
    }

    internal void SendPacket(IpPacket packet)
    {
        packet.UpdateAllChecksums();
        OnPacketSend?.Invoke(packet);
    }

    private LocalTcpListener? ResolveListener(IpEndPointValue endPoint)
    {
        // If no specific listeners, use the "any" listener if available
        if (_listeners.IsEmpty)
            return _anyListener;

        // Try to find a specific listener first
        return _listeners.TryGetValue(endPoint, out var listener) ? listener : _anyListener;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var kvp in _listeners)
        {
            if (_listeners.TryRemove(kvp.Key, out var listener))
                listener.Dispose();
        }

        _anyListener?.Dispose();
        _anyListener = null;

        foreach (var kvp in _connections)
            if (_connections.TryRemove(kvp.Key, out var connection))
                connection.Dispose();
    }
}
