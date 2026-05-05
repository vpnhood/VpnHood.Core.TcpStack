using System.Threading.Channels;
using VpnHood.Core.TcpStack.Abstractions;

namespace VpnHood.Core.TcpStack.LwIP;

/// <summary>
/// TCP listener for <see cref="LwipTcpStack"/> that accepts incoming connections.
/// </summary>
public sealed class LwipTcpListener : ITcpListener
{
    private readonly Channel<LwipTcpStream> _acceptQueue;
    private readonly LwipTcpStack _stack;
    private int _stopped;

    internal LwipTcpListener(LwipTcpStack stack, Channel<LwipTcpStream> acceptQueue)
    {
        _stack = stack;
        _acceptQueue = acceptQueue;
    }

    internal bool TryEnqueueAccept(LwipTcpStream stream)
    {
        if (Volatile.Read(ref _stopped) != 0) return false;
        return _acceptQueue.Writer.TryWrite(stream);
    }

    /// <summary>
    /// Asynchronously accepts all incoming connections.
    /// </summary>
    public IAsyncEnumerable<LwipTcpStream> AcceptAllAsync(CancellationToken cancellationToken = default)
    {
        return _acceptQueue.Reader.ReadAllAsync(cancellationToken);
    }

    async IAsyncEnumerable<ITcpClient> ITcpListener.AcceptAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var stream in AcceptAllAsync(cancellationToken).ConfigureAwait(false))
            yield return stream;
    }

    /// <summary>
    /// Asynchronously accepts a single incoming connection.
    /// </summary>
    public ValueTask<LwipTcpStream> AcceptAsync(CancellationToken cancellationToken = default)
    {
        return _acceptQueue.Reader.ReadAsync(cancellationToken);
    }

    async ValueTask<ITcpClient> ITcpListener.AcceptAsync(CancellationToken cancellationToken)
    {
        return await AcceptAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Stops the listener and disposes any unaccepted connections.
    /// </summary>
    public void Stop()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0) return;
        _stack.StopListening();
        _acceptQueue.Writer.TryComplete();
        while (_acceptQueue.Reader.TryRead(out var stream))
            stream.Dispose();
    }

    /// <inheritdoc />
    public void Dispose() => Stop();
}
