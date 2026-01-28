using System.IO.Pipelines;
using System.Runtime.InteropServices;
using VpnHood.Core.Toolkit.Utils;

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
            // Complete the pipe when done
            await _readPipe.Writer.CompleteAsync();
        }
        catch (OperationCanceledException) when (_disposed)
        {
            // Ignore cancellation during disposal and complete the pipe
            await _readPipe.Writer.CompleteAsync();
        }
    }

    public override bool CanRead => !_disposed;

    public override bool CanWrite => !_disposed;

    public override bool CanSeek => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Read is not supported. Use ReadAsync instead.");
    }

    /// <inheritdoc />
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }

    /// <inheritdoc />
    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Read data from the internal pipe
        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        var readResult = await _readPipe.Reader.ReadAsync(combinedCts.Token).Vhc();
        if (readResult.IsCanceled || readResult is { IsCompleted: true, Buffer.IsEmpty: true })
            return 0;

        // Copy data to the provided buffer
        var bytesToCopy = Math.Min(buffer.Length, (int)readResult.Buffer.Length);
        var sliceToRead = readResult.Buffer.Slice(0, bytesToCopy);
        var copied = 0;
        foreach (var segment in sliceToRead)
        {
            var toCopy = Math.Min(segment.Length, bytesToCopy - copied);
            segment.Span[..toCopy].CopyTo(buffer.Span.Slice(copied));
            copied += toCopy;
            if (copied >= bytesToCopy) break;
        }

        // Advance the pipe reader
        _readPipe.Reader.AdvanceTo(readResult.Buffer.GetPosition(bytesToCopy));
        return bytesToCopy;
    }


    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Write is not supported. Use WriteAsync instead.");
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


    public override void Flush() { }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

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
