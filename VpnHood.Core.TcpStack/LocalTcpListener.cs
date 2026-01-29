using System.Net;
using System.Threading.Channels;

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
    private bool _stopped;

    /// <summary>
    /// The local endpoint this listener is bound to.
    /// </summary>
    public IPEndPoint LocalEndPoint { get; }

    internal LocalTcpListener(LocalTcpStack stack, IPEndPoint localEndPoint)
    {
        _stack = stack;
        LocalEndPoint = localEndPoint;
    }

    internal void EnqueueAccept(LocalTcpConnection connection)
    {
        if (_stopped) return;
        
        var stream = new LocalTcpStream(connection, _stack);
        if (!_acceptQueue.Writer.TryWrite(stream))
        {
            // Queue was completed, dispose the stream
            stream.Dispose();
        }
    }

    /// <summary>
    /// Asynchronously accepts all incoming connections.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token to stop accepting connections.</param>
    /// <returns>An async enumerable of connected streams.</returns>
    public IAsyncEnumerable<LocalTcpStream> AcceptAllAsync(CancellationToken cancellationToken = default)
    {
        return _acceptQueue.Reader.ReadAllAsync(cancellationToken);
    }

    /// <summary>
    /// Asynchronously accepts a single incoming connection.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The accepted stream.</returns>
    public async ValueTask<LocalTcpStream> AcceptAsync(CancellationToken cancellationToken = default)
    {
        return await _acceptQueue.Reader.ReadAsync(cancellationToken);
    }

    /// <summary>
    /// Stops the listener and completes the accept queue.
    /// </summary>
    public void Stop()
    {
        if (_stopped) return;
        _stopped = true;
        
        _acceptQueue.Writer.TryComplete();
        _stack.StopListening(LocalEndPoint);
        
        // Dispose any unaccepted streams
        while (_acceptQueue.Reader.TryRead(out var stream))
            stream.Dispose();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Stop();
    }
}
