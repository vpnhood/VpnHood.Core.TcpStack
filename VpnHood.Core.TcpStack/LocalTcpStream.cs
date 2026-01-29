using System.Buffers;

namespace VpnHood.Core.TcpStack;

/// <summary>
/// A standard .NET Stream implementation for TCP connections through the local TCP stack.
/// Provides async read/write operations over TCP connections using System.IO.Pipelines for efficiency.
/// </summary>
public sealed class LocalTcpStream : Stream
{
    private readonly LocalTcpConnection _connection;
    private readonly LocalTcpStack _stack;
    private readonly CancellationTokenSource _cts = new();
    private bool _disposed;

    internal LocalTcpStream(LocalTcpConnection connection, LocalTcpStack stack)
    {
        _connection = connection;
        _stack = stack;
    }

    /// <inheritdoc />
    public override bool CanRead => !_disposed;

    /// <inheritdoc />
    public override bool CanWrite => !_disposed;

    /// <inheritdoc />
    public override bool CanSeek => false;

    /// <inheritdoc />
    public override long Length => throw new NotSupportedException();

    /// <inheritdoc />
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Read is not supported in synchronous mode. Use ReadAsync instead.");
    }

    /// <inheritdoc />
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    /// <inheritdoc />
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);

        // Read directly from the connection's pipe - no intermediate buffer
        var reader = _connection.NetToAppReader;
        var readResult = await reader.ReadAsync(combinedCts.Token);

        if (readResult.IsCanceled || (readResult.IsCompleted && readResult.Buffer.IsEmpty))
            return 0;

        var bytesToCopy = (int)Math.Min(buffer.Length, readResult.Buffer.Length);

        // Copy directly from ReadOnlySequence to destination span
        readResult.Buffer.Slice(0, bytesToCopy).CopyTo(buffer.Span);

        reader.AdvanceTo(readResult.Buffer.GetPosition(bytesToCopy));
        
        return bytesToCopy;
    }

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Write is not supported in synchronous mode. Use WriteAsync instead.");
    }

    /// <inheritdoc />
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    /// <inheritdoc />
    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);

        // Write directly to the connection's pipe - zero copy
        await _connection.SendAppDataAsync(buffer, combinedCts.Token);
    }

    /// <inheritdoc />
    public override void Flush() { }

    /// <inheritdoc />
    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    /// <inheritdoc />
    public override void SetLength(long value) => throw new NotSupportedException();

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            _disposed = true;
            _cts.Cancel();
            _connection.StartFin(_stack);
            _cts.Dispose();
        }
        base.Dispose(disposing);
    }
}
