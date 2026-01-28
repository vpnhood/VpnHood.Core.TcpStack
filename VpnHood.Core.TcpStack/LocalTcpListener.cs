using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;

namespace VpnHood.Core.TcpStack;

/// <summary>
/// TCP listener that accepts incoming connections on a local endpoint.
/// Similar to <see cref="System.Net.Sockets.TcpListener"/> but for the local TCP stack.
/// </summary>
public sealed class LocalTcpListener
{
    private readonly Channel<LocalTcpStream> _acceptQueue = Channel.CreateUnbounded<LocalTcpStream>(
        new UnboundedChannelOptions { SingleReader = true });

    private readonly LocalTcpStack _stack;

    /// <summary>
    /// The local endpoint this listener is bound to.
    /// </summary>
    public IPEndPoint LocalEndPoint { get; }

    internal LocalTcpListener(LocalTcpStack stack, IPEndPoint localEndPoint, ConcurrentDictionary<Quad, LocalTcpConnection> connections)
    {
        _stack = stack;
        LocalEndPoint = localEndPoint;
    }

    internal void EnqueueAccept(LocalTcpConnection conn)
    {
        var stream = new LocalTcpStream(conn, _stack);
        _acceptQueue.Writer.TryWrite(stream);
    }

    /// <summary>
    /// Asynchronously accepts all incoming connections.
    /// </summary>
    /// <param name="ct">Cancellation token to stop accepting connections.</param>
    /// <returns>An async enumerable of connected streams.</returns>
    public IAsyncEnumerable<LocalTcpStream> AcceptAllAsync(CancellationToken ct = default) => _acceptQueue.Reader.ReadAllAsync(ct);

    /// <summary>
    /// Asynchronously accepts a single incoming connection.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The accepted stream.</returns>
    public async ValueTask<LocalTcpStream> AcceptAsync(CancellationToken ct = default)
    {
        return await _acceptQueue.Reader.ReadAsync(ct);
    }

    /// <summary>
    /// Stops the listener and completes the accept queue.
    /// </summary>
    public void Stop()
    {
        _acceptQueue.Writer.TryComplete();
        _stack.StopListening(LocalEndPoint);
    }
}
