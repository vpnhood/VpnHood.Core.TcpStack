using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;

namespace VpnHood.Core.TcpStack;

public sealed class LocalTcpListener
{
    private readonly Channel<LocalTcpStream> _acceptQueue = Channel.CreateUnbounded<LocalTcpStream>(new UnboundedChannelOptions { SingleReader = true });
    private readonly ConcurrentDictionary<Quad, LocalTcpConnection> _connections;
    private readonly LocalTcpStack _stack;
    
    public IPEndPoint LocalEndPoint { get; }

    internal LocalTcpListener(LocalTcpStack stack, IPEndPoint localEndPoint, ConcurrentDictionary<Quad, LocalTcpConnection> connections)
    {
        _stack = stack;
        LocalEndPoint = localEndPoint;
        _connections = connections;
    }

    internal void EnqueueAccept(LocalTcpConnection conn)
    {
        var stream = new LocalTcpStream(conn, _stack);
        _acceptQueue.Writer.TryWrite(stream);
    }

    public IAsyncEnumerable<LocalTcpStream> AcceptAllAsync(CancellationToken ct = default) => _acceptQueue.Reader.ReadAllAsync(ct);
}
