using System.Threading.Channels;
using VpnHood.Core.Toolkit.Net;

namespace VpnHood.Core.TcpStack;

/// <summary>
/// TCP listener that accepts incoming connections on a local endpoint.
/// Similar to <see cref="System.Net.Sockets.TcpListener"/> but for the local TCP stack.
/// </summary>
public sealed class LocalTcpListener : IDisposable
{
    private readonly Channel<LocalTcpStream> _acceptQueue = Channel.CreateUnbounded<LocalTcpStream>(
        new UnboundedChannelOptions { SingleReader = true });

    private readonly LocalTcpStack _stack;
    private int _stopped;

    /// <summary>
    /// The local endpoint this listener is bound to. Null = wildcard listener (any IPv4/IPv6).
    /// </summary>
    public IpEndPointValue? LocalEndPoint { get; }

    /// <summary>
    /// True when this listener accepts connections on any endpoint.
    /// </summary>
    public bool IsAny => LocalEndPoint is null;

    internal LocalTcpListener(LocalTcpStack stack, IpEndPointValue? localEndPoint)
    {
        _stack = stack;
        LocalEndPoint = localEndPoint;
    }

    /// <summary>
    /// Try to enqueue an accepted stream. Returns false if the listener has been stopped,
    /// in which case the caller is responsible for disposing the stream.
    /// </summary>
    internal bool TryEnqueueAccept(LocalTcpStream stream)
    {
        if (Volatile.Read(ref _stopped) != 0) return false;
        return _acceptQueue.Writer.TryWrite(stream);
    }

    /// <summary>
    /// Asynchronously accepts all incoming connections.
    /// </summary>
    public IAsyncEnumerable<LocalTcpStream> AcceptAllAsync(CancellationToken cancellationToken = default)
    {
        return _acceptQueue.Reader.ReadAllAsync(cancellationToken);
    }

    /// <summary>
    /// Asynchronously accepts a single incoming connection.
    /// </summary>
    public ValueTask<LocalTcpStream> AcceptAsync(CancellationToken cancellationToken = default)
    {
        return _acceptQueue.Reader.ReadAsync(cancellationToken);
    }

    /// <summary>
    /// Stops the listener and completes the accept queue. Disposes any unaccepted streams.
    /// </summary>
    public void Stop()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0) return;

        _acceptQueue.Writer.TryComplete();

        if (LocalEndPoint.HasValue)
            _stack.StopListening(LocalEndPoint.Value);
        else
            _stack.StopListeningAny();

        // Dispose any unaccepted streams to release their connections.
        while (_acceptQueue.Reader.TryRead(out var stream))
            stream.Dispose();
    }

    /// <inheritdoc />
    public void Dispose() => Stop();
}
