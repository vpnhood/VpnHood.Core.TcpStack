using System.IO.Pipelines;

namespace VpnHood.Core.TcpStack;

/// <summary>
/// A standard .NET Stream implementation for TCP connections through the local TCP stack.
/// Provides async read/write operations over TCP connections.
/// </summary>
public sealed class LocalTcpStream : Stream
{
    private readonly LocalTcpConnection _connection;
    private readonly LocalTcpStack _stack;
    private readonly CancellationTokenSource _cts = new();
    private readonly Pipe _readPipe = new();
    private bool _disposed;

    internal LocalTcpStream(LocalTcpConnection connection, LocalTcpStack stack)
    {
        _connection = connection;
        _stack = stack;
        _ = Task.Run(PumpIncomingAsync);
    }

    private async Task PumpIncomingAsync()
    {
        try
        {
            await foreach (var chunk in _connection.ReadAppDataAsync(_cts.Token))
            {
                await _readPipe.Writer.WriteAsync(chunk, _cts.Token);
                await _readPipe.Writer.FlushAsync(_cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when stream is disposed
        }
        catch
        {
            // Silently ignore other errors
        }
        finally
        {
            await _readPipe.Writer.CompleteAsync();
        }
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
        return ReadAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        var readResult = await _readPipe.Reader.ReadAsync(combinedCts.Token);

        if (readResult.IsCanceled || (readResult.IsCompleted && readResult.Buffer.IsEmpty))
            return 0;

        var bytesToCopy = Math.Min(count, (int)readResult.Buffer.Length);
        var sliceToRead = readResult.Buffer.Slice(0, bytesToCopy);

        // Copy from ReadOnlySequence to the target buffer
        var copied = 0;
        foreach (var segment in sliceToRead)
        {
            var toCopy = Math.Min(segment.Length, bytesToCopy - copied);
            segment.Span[..toCopy].CopyTo(buffer.AsSpan(offset + copied));
            copied += toCopy;
            if (copied >= bytesToCopy) break;
        }

        _readPipe.Reader.AdvanceTo(readResult.Buffer.GetPosition(bytesToCopy));
        return bytesToCopy;
    }

    /// <inheritdoc />
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return new ValueTask<int>(ReadAsync(buffer.ToArray(), 0, buffer.Length, cancellationToken));
    }

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count)
    {
        WriteAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var data = new byte[count];
        Buffer.BlockCopy(buffer, offset, data, 0, count);

        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        await _connection.SendAppDataAsync(data, combinedCts.Token);
    }

    /// <inheritdoc />
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return new ValueTask(WriteAsync(buffer.ToArray(), 0, buffer.Length, cancellationToken));
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
