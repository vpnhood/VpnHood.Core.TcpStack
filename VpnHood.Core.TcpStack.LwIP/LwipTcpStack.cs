using System.Collections.Concurrent;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using VpnHood.Core.Packets;
using VpnHood.Core.TcpStack.Abstractions;
using VpnHood.Core.Toolkit.Net;

namespace VpnHood.Core.TcpStack.LwIP;

/// <summary>
/// A TCP stack implementation backed by the lwIP C library.
/// Provides the same listener/stream pattern as <see cref="LocalTcpStack"/>
/// but delegates TCP state management to lwIP.
/// </summary>
public sealed class LwipTcpStack : ITcpStack
{
    private readonly nint _stack;
    private readonly ConcurrentDictionary<nint, LwipTcpConnection> _connections = new();
    private readonly Channel<LwipTcpStream> _acceptQueue = Channel.CreateUnbounded<LwipTcpStream>(
        new UnboundedChannelOptions { SingleReader = false });
    private readonly Lock _listenerLock = new();
    private readonly Timer _pollTimer;
    private LwipTcpListener? _tcpListener;
    private nint _listener;
    private bool _disposed;

    // Must keep delegates alive to prevent GC collection while native code holds pointers.
    private readonly LwipNative.OutputCallback _outputDelegate;
    private readonly LwipNative.AcceptCallback _acceptDelegate;
    private readonly LwipNative.RecvCallback _recvDelegate;
    private readonly LwipNative.ClosedCallback _closedDelegate;
    private readonly LwipNative.SentCallback _sentDelegate;

    /// <summary>
    /// Callback invoked when a TCP packet needs to be sent out.
    /// The callback receives the raw IP packet bytes.
    /// </summary>
    public Action<IpPacket>? OnPacketSend { get; set; }

    public LwipTcpStack()
    {
        _stack = LwipNative.Create();
        if (_stack == 0)
            throw new InvalidOperationException("Failed to create lwIP stack.");

        // Wire up callbacks (prevent GC)
        unsafe {
            _outputDelegate = OnNativeOutput;
            _acceptDelegate = OnNativeAccept;
            _recvDelegate = OnNativeReceive;
        }
        _closedDelegate = OnNativeClosed;
        _sentDelegate = OnNativeSent;

        LwipNative.SetOutputCallback(_stack, _outputDelegate, 0);
        LwipNative.SetAcceptCallback(_stack, _acceptDelegate, 0);
        LwipNative.SetRecvCallback(_stack, _recvDelegate);
        LwipNative.SetClosedCallback(_stack, _closedDelegate);
        LwipNative.SetSentCallback(_stack, _sentDelegate);

        // Poll lwIP timers every 5ms
        _pollTimer = new Timer(_ => PollTimers(), null, TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(5));
    }

    /// <summary>
    /// Starts listening for incoming TCP connections on any address/port.
    /// </summary>
    public LwipTcpListener ListenAny()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        lock (_listenerLock) {
            if (_tcpListener != null) return _tcpListener;
            if (_listener == 0)
                _listener = LwipNative.ListenAny(_stack);
            _tcpListener = new LwipTcpListener(this, _acceptQueue);
            return _tcpListener;
        }
    }

    ITcpListener ITcpStack.ListenAny() => ListenAny();

    internal void StopListening()
    {
        lock (_listenerLock) {
            _tcpListener = null;
            if (_listener != 0) {
                LwipNative.StopListen(_stack, _listener);
                _listener = 0;
            }
        }
    }

    /// <summary>
    /// Asynchronously accepts incoming TCP connections.
    /// </summary>
    public IAsyncEnumerable<LwipTcpStream> AcceptAllAsync(CancellationToken cancellationToken = default)
    {
        return ListenAny().AcceptAllAsync(cancellationToken);
    }

    /// <summary>
    /// Accepts a single incoming connection.
    /// </summary>
    public ValueTask<LwipTcpStream> AcceptAsync(CancellationToken cancellationToken = default)
    {
        return ListenAny().AcceptAsync(cancellationToken);
    }

    /// <summary>
    /// Feeds a raw IP packet into the lwIP stack for processing.
    /// </summary>
    public void ProcessIncoming(ReadOnlySpan<byte> packetData)
    {
        if (_disposed) return;
        unsafe {
            fixed (byte* ptr = packetData) {
                LwipNative.Input(_stack, ptr, packetData.Length);
            }
        }
    }

    /// <summary>
    /// Feeds an already-parsed IP packet into the stack.
    /// </summary>
    public void ProcessIncoming(IpPacket ipPacket)
    {
        if (_disposed) return;
        if (ipPacket.Protocol != IpProtocol.Tcp) return;

        var span = ipPacket.Buffer.Span;
        unsafe {
            fixed (byte* ptr = span) {
                LwipNative.Input(_stack, ptr, span.Length);
            }
        }
    }

    /// <summary>
    /// Writes data to a connection. Returns the number of bytes actually enqueued.
    /// </summary>
    internal int Write(nint conn, ReadOnlySpan<byte> data)
    {
        if (_disposed || conn == 0) return -1;
        unsafe {
            fixed (byte* ptr = data) {
                return LwipNative.Write(_stack, conn, ptr, data.Length);
            }
        }
    }

    /// <summary>
    /// Gets available send buffer space for a connection.
    /// </summary>
    internal int GetSendBufferSpace(nint conn)
    {
        if (_disposed || conn == 0) return 0;
        return LwipNative.SndBuf(_stack, conn);
    }

    /// <summary>
    /// Gracefully closes a connection.
    /// </summary>
    internal void CloseConnection(nint conn)
    {
        if (_disposed || conn == 0) return;
        _connections.TryRemove(conn, out _);
        LwipNative.Close(_stack, conn);
    }

    /// <summary>
    /// Aborts a connection (sends RST).
    /// </summary>
    internal void AbortConnection(nint conn)
    {
        if (_disposed || conn == 0) return;
        _connections.TryRemove(conn, out _);
        LwipNative.Abort(_stack, conn);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _pollTimer.Dispose();

        // Stop the listener (completes the accept queue and stops native listening)
        _tcpListener?.Stop();
        _tcpListener = null;

        // Ensure accept queue is completed even if no listener was created
        _acceptQueue.Writer.TryComplete();
        while (_acceptQueue.Reader.TryRead(out var stream))
            stream.Dispose();

        // Dispose all active connections
        foreach (var kvp in _connections) {
            kvp.Value.Dispose();
        }
        _connections.Clear();

        // Stop any native listener not managed by LwipTcpListener
        if (_listener != 0) {
            LwipNative.StopListen(_stack, _listener);
            _listener = 0;
        }

        LwipNative.Destroy(_stack);
    }

    private void PollTimers()
    {
        if (_disposed) return;
        LwipNative.Poll(_stack);
    }

    // --- Native callbacks ---

    private unsafe void OnNativeOutput(byte* data, int len, nint userData)
    {
        if (_disposed || OnPacketSend == null) return;

        var span = new ReadOnlySpan<byte>(data, len);
        IpPacket? packet = null;
        try {
            packet = PacketBuilder.Parse(span);
            OnPacketSend(packet);
        }
        catch {
            packet?.Dispose();
        }
    }

    private unsafe nint OnNativeAccept(
        nint conn,
        uint localIp4, ushort localPort,
        uint remoteIp4, ushort remotePort,
        byte* localIp6, byte* remoteIp6,
        int isIpv6,
        nint userData)
    {
        if (_disposed) return 0;

        IPEndPoint localEp, remoteEp;

        if (isIpv6 != 0 && localIp6 != null && remoteIp6 != null) {
            var localBytes = new ReadOnlySpan<byte>(localIp6, 16);
            var remoteBytes = new ReadOnlySpan<byte>(remoteIp6, 16);
            localEp = new IPEndPoint(new IPAddress(localBytes), localPort);
            remoteEp = new IPEndPoint(new IPAddress(remoteBytes), remotePort);
        }
        else {
            // IPv4 stored as network byte order u32
            localEp = new IPEndPoint(new IPAddress(localIp4), localPort);
            remoteEp = new IPEndPoint(new IPAddress(remoteIp4), remotePort);
        }

        var connection = new LwipTcpConnection(conn, this, localEp, remoteEp);
        _connections[conn] = connection;

        var stream = new LwipTcpStream(connection, localEp, remoteEp);
        _acceptQueue.Writer.TryWrite(stream);
        // Return connection handle as the "user context" for recv/closed callbacks
        var gcHandle = GCHandle.Alloc(connection);
        return GCHandle.ToIntPtr(gcHandle);
    }

    private static unsafe int OnNativeReceive(nint conn, byte* data, int len, nint connCtx)
    {
        if (connCtx == 0) return len; // acknowledge but discard

        var gcHandle = GCHandle.FromIntPtr(connCtx);
        if (!gcHandle.IsAllocated) return len;

        var connection = (LwipTcpConnection)gcHandle.Target!;
        var span = new ReadOnlySpan<byte>(data, len);
        return connection.OnDataReceived(span);
    }

    private void OnNativeClosed(nint conn, int err, nint connCtx)
    {
        if (connCtx == 0) return;

        var gcHandle = GCHandle.FromIntPtr(connCtx);
        if (!gcHandle.IsAllocated) return;

        var connection = (LwipTcpConnection)gcHandle.Target!;
        connection.OnRemoteClosed(err);
        _connections.TryRemove(conn, out _);
        gcHandle.Free();
    }

    private static void OnNativeSent(nint conn, int len, nint connCtx)
    {
        if (connCtx == 0) return;

        var gcHandle = GCHandle.FromIntPtr(connCtx);
        if (!gcHandle.IsAllocated) return;

        var connection = (LwipTcpConnection)gcHandle.Target!;
        connection.OnSendBufferAvailable(len);
    }
}
