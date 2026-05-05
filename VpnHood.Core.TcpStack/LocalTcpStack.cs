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
/// <remarks>
/// IMPORTANT: <see cref="OnPacketSend"/> hands ownership of the produced <see cref="IpPacket"/>
/// to the consumer. The consumer is responsible for disposing it (the WinDivert adapter does
/// this automatically when AutoDisposePackets = true). When no consumer is registered, the
/// stack disposes the packet itself to release pooled memory.
/// </remarks>
public sealed class LocalTcpStack : IDisposable
{
    // Fixed window size for loopback - no need for large windows since transfer is instant
    private const ushort LoopbackWindowSize = 65535;

    private readonly ConcurrentDictionary<IpEndPointQuad, LocalTcpConnection> _connections = new();
    private readonly ConcurrentDictionary<IpEndPointValue, LocalTcpListener> _listeners = new();
    private readonly Lock _anyListenerLock = new();
    private LocalTcpListener? _anyListener;
    private bool _disposed;

    /// <summary>
    /// Callback invoked when a TCP packet needs to be sent out. The callback takes ownership of the packet.
    /// </summary>
    public Action<IpPacket>? OnPacketSend { get; set; }

    /// <summary>
    /// Creates a TCP listener on the specified local endpoint.
    /// </summary>
    public LocalTcpListener Listen(IpEndPointValue localEndPoint)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _listeners.GetOrAdd(localEndPoint, ep => new LocalTcpListener(this, ep));
    }

    /// <summary>
    /// Creates a TCP listener that accepts connections on any endpoint (IPv4 and IPv6).
    /// </summary>
    public LocalTcpListener ListenAny()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_anyListenerLock) {
            return _anyListener ??= new LocalTcpListener(this, null);
        }
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
        return _listeners.TryRemove(localEndPoint, out _);
    }

    /// <summary>
    /// Stops the wildcard listener that accepts connections on any endpoint.
    /// </summary>
    internal bool StopListeningAny()
    {
        lock (_anyListenerLock) {
            if (_anyListener == null)
                return false;

            _anyListener = null;
            return true;
        }
    }

    /// <summary>
    /// Processes an incoming IP packet. The bytes are copied into a pooled buffer; the original
    /// span is not retained, and the pooled buffer is released before this method returns.
    /// </summary>
    public void ProcessIncoming(ReadOnlySpan<byte> packetData)
    {
        if (_disposed)
            return;

        IpPacket? ipPacket = null;
        try {
            ipPacket = PacketBuilder.Parse(packetData);
            ProcessIncomingInternal(ipPacket);
        }
        catch {
            // Silently ignore malformed packets
        }
        finally {
            ipPacket?.Dispose();
        }
    }

    /// <summary>
    /// Processes an already-parsed incoming IP packet without taking ownership.
    /// The caller remains responsible for disposing <paramref name="ipPacket"/>.
    /// </summary>
    public void ProcessIncoming(IpPacket ipPacket)
    {
        if (_disposed)
            return;

        try {
            ProcessIncomingInternal(ipPacket);
        }
        catch {
            // Silently ignore malformed packets
        }
    }

    private void ProcessIncomingInternal(IpPacket ipPacket)
    {
        if (ipPacket.Protocol != IpProtocol.Tcp)
            return;

        var tcpPacket = ipPacket.ExtractTcp();
        var endPointQuad = new IpEndPointQuad(
            new IpEndPointValue(ipPacket.SourceAddress, tcpPacket.SourcePort),
            new IpEndPointValue(ipPacket.DestinationAddress, tcpPacket.DestinationPort));

        // Handle SYN packets (new connection requests)
        if (tcpPacket is { Synchronize: true, Acknowledgment: false }) {
            HandleSynPacket(endPointQuad, tcpPacket);
            return;
        }

        // Handle packets for existing connections
        if (_connections.TryGetValue(endPointQuad, out var existing)) {
            HandleExistingConnection(existing, tcpPacket);
            return;
        }

        // Unknown connection - send RST if not already an RST
        if (!tcpPacket.Reset)
            SendRst(endPointQuad.Destination, endPointQuad.Source, tcpPacket);
    }

    private void HandleSynPacket(IpEndPointQuad endPointQuad, TcpPacket tcpPacket)
    {
        var listener = ResolveListener(endPointQuad.Destination);
        if (listener == null) {
            // No listener - send RST
            SendRst(endPointQuad.Destination, endPointQuad.Source, tcpPacket);
            return;
        }

        // SYN retransmit for a connection that's still in SynReceived: re-send SYN-ACK
        if (_connections.TryGetValue(endPointQuad, out var existingConn)) {
            if (existingConn.State == TcpConnectionState.SynReceived)
                SendSynAck(existingConn);
            return;
        }

        var isnLocal = (uint)RandomNumberGenerator.GetInt32(int.MaxValue);
        var peerMss = ParseMssOption(tcpPacket.Options.Span);
        var connection = new LocalTcpConnection(endPointQuad, isnLocal, tcpPacket.SequenceNumber, peerMss, listener);
        connection.OnClosed += OnConnectionClosed;

        if (!_connections.TryAdd(endPointQuad, connection)) {
            connection.Dispose();
            return;
        }

        SendSynAck(connection);

        // Note: do NOT enqueue accept yet. The listener gets the stream only after the
        // final ACK arrives and the connection transitions to Established.
        connection.Start(this);
    }

    private void SendSynAck(LocalTcpConnection conn)
    {
        var packet = PacketBuilder.BuildTcp(
            conn.EndPointQuad.Destination, conn.EndPointQuad.Source,
            options: ReadOnlySpan<byte>.Empty,
            payload: ReadOnlySpan<byte>.Empty);

        var tcp = packet.ExtractTcp();
        // Idempotent across SYN retransmits: SndNxt must be ISN+1 once SYN-ACK is sent.
        conn.SetSndNxtAfterSyn();
        var (_, rcvNxt) = conn.SnapshotSequence();
        tcp.SequenceNumber = conn.IsnLocal;
        tcp.AcknowledgmentNumber = rcvNxt;
        tcp.Synchronize = true;
        tcp.Acknowledgment = true;
        tcp.WindowSize = conn.CurrentWindowSize;

        SendPacket(packet);
    }

    private void HandleExistingConnection(LocalTcpConnection conn, TcpPacket tcpPacket)
    {
        // Transition from SynReceived to Established on first valid ACK
        if (conn.State == TcpConnectionState.SynReceived && tcpPacket.Acknowledgment)
            conn.MarkEstablished();

        var flags = (TcpFlags)0;
        if (tcpPacket.Finish) flags |= TcpFlags.Fin;
        if (tcpPacket.Reset) flags |= TcpFlags.Rst;
        if (tcpPacket.Acknowledgment) flags |= TcpFlags.Ack;

        var (handled, needsAck) = conn.TryHandleIncoming(
            tcpPacket.SequenceNumber,
            tcpPacket.AcknowledgmentNumber,
            tcpPacket.WindowSize,
            flags,
            tcpPacket.Payload.Span);

        if (!handled || !needsAck)
            return;

        SendAckOnly(conn);
    }

    internal void SendAckOnly(LocalTcpConnection conn)
    {
        var packet = PacketBuilder.BuildTcp(
            conn.EndPointQuad.Destination, conn.EndPointQuad.Source,
            options: ReadOnlySpan<byte>.Empty,
            payload: ReadOnlySpan<byte>.Empty);

        var tcp = packet.ExtractTcp();
        var (sndNxt, rcvNxt) = conn.SnapshotSequence();
        tcp.SequenceNumber = sndNxt;
        tcp.AcknowledgmentNumber = rcvNxt;
        tcp.Acknowledgment = true;
        tcp.WindowSize = conn.CurrentWindowSize;

        SendPacket(packet);
    }

    private void SendRst(IpEndPointValue localEndPoint, IpEndPointValue remoteEndPoint, TcpPacket incomingTcp)
    {
        var rstPacket = PacketBuilder.BuildTcp(
            localEndPoint, remoteEndPoint,
            options: ReadOnlySpan<byte>.Empty,
            payload: ReadOnlySpan<byte>.Empty);

        var rstTcp = rstPacket.ExtractTcp();
        rstTcp.Reset = true;

        // RFC 793: If ACK bit is off, seq = 0, ack = seq + segment length
        // If ACK bit is on, seq = ack number from incoming
        if (incomingTcp.Acknowledgment) {
            rstTcp.SequenceNumber = incomingTcp.AcknowledgmentNumber;
        }
        else {
            rstTcp.SequenceNumber = 0;
            var ackNum = incomingTcp.SequenceNumber + (uint)incomingTcp.Payload.Length;
            if (incomingTcp.Synchronize) ackNum += 1;
            if (incomingTcp.Finish) ackNum += 1;
            rstTcp.AcknowledgmentNumber = ackNum;
            rstTcp.Acknowledgment = true;
        }

        SendPacket(rstPacket);
    }

    private void OnConnectionClosed(LocalTcpConnection conn)
    {
        _connections.TryRemove(conn.EndPointQuad, out _);
    }

    /// <summary>
    /// Closes all currently active connections. New connections can still be accepted afterward.
    /// </summary>
    public void DropAllConnections()
    {
        foreach (var kvp in _connections) {
            if (_connections.TryRemove(kvp.Key, out var connection))
                connection.Dispose();
        }
    }

    /// <summary>
    /// Updates checksums and hands the packet to the consumer. If no consumer is registered the
    /// pooled buffer is released so we never leak memory.
    /// </summary>
    internal void SendPacket(IpPacket packet)
    {
        var callback = OnPacketSend;
        if (callback == null) {
            packet.Dispose();
            return;
        }

        try {
            packet.UpdateAllChecksums();
            callback(packet);
        }
        catch {
            // If the consumer threw, dispose to avoid leaking the pooled buffer.
            try { packet.Dispose(); } catch { /* ignore */ }
            throw;
        }
    }

    private LocalTcpListener? ResolveListener(IpEndPointValue endPoint)
    {
        // Specific listener wins over wildcard
        if (_listeners.TryGetValue(endPoint, out var listener))
            return listener;

        return _anyListener;
    }

    /// <summary>
    /// Parses the TCP "Maximum Segment Size" option (kind=2, len=4) from the SYN options.
    /// Returns null when the option is absent or malformed.
    /// </summary>
    private static ushort? ParseMssOption(ReadOnlySpan<byte> options)
    {
        var i = 0;
        while (i < options.Length) {
            var kind = options[i];
            switch (kind) {
                case 0: // End of option list
                    return null;
                case 1: // NOP - single byte
                    i++;
                    continue;
            }

            // Multibyte option: must have at least the length byte
            if (i + 1 >= options.Length) return null;
            var len = options[i + 1];
            if (len < 2 || i + len > options.Length) return null;

            if (kind == 2 && len == 4)
                return (ushort)((options[i + 2] << 8) | options[i + 3]);

            i += len;
        }

        return null;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var kvp in _listeners) {
            if (_listeners.TryRemove(kvp.Key, out var listener))
                listener.Dispose();
        }

        LocalTcpListener? anyListener;
        lock (_anyListenerLock) {
            anyListener = _anyListener;
            _anyListener = null;
        }
        anyListener?.Dispose();

        foreach (var kvp in _connections) {
            if (_connections.TryRemove(kvp.Key, out var connection))
                connection.Dispose();
        }
    }
}
