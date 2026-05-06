using System.Buffers;

namespace VpnHood.Core.TcpStack.LwIP;

/// <summary>
/// A standard .NET Stream implementation for TCP connections through the lwIP stack.
/// </summary>
public sealed class LwipTcpStream : Stream
{
    private readonly LwipTcpConnection _connection;
    private int _disposed;

    internal LwipTcpStream(LwipTcpConnection connection)
    {
        _connection = connection;
    }

    private bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    public override bool CanRead => !IsDisposed;
    public override bool CanWrite => !IsDisposed;
    public override bool CanSeek => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Use ReadAsync instead.");
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);

        var reader = _connection.ReceiveReader;
        var result = await reader.ReadAsync(cancellationToken);

        if (result.IsCanceled || result is { IsCompleted: true, Buffer.IsEmpty: true })
            return 0;

        var bytesToCopy = (int)Math.Min(buffer.Length, result.Buffer.Length);
        var source = result.Buffer.Slice(0, bytesToCopy);
        source.CopyTo(buffer.Span[..bytesToCopy]);
        reader.AdvanceTo(result.Buffer.GetPosition(bytesToCopy));
        return bytesToCopy;
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Use WriteAsync instead.");
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        await _connection.WriteAsync(buffer, cancellationToken);
    }

    public override void Flush() { }
    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        if (disposing) {
            _connection.Close();
        }
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return ValueTask.CompletedTask;

        _connection.Close();
        return ValueTask.CompletedTask;
    }
}
