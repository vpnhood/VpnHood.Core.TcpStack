using System.Buffers;
using System.IO.Pipelines;

namespace VpnHood.Core.TcpStack;

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
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
        }
        finally
        {
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
        return ReadAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        var readResult = await _readPipe.Reader.ReadAsync(combinedCts.Token);

        if (readResult.IsCanceled || readResult.IsCompleted && readResult.Buffer.IsEmpty)
            return 0;

        var bytesToCopy = Math.Min(count, (int)readResult.Buffer.Length);
        var sliceToRead = readResult.Buffer.Slice(0, bytesToCopy);

        // Copy from ReadOnlySequence to the target buffer using a more compatible approach
        var position = sliceToRead.Start;
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

    public override void Write(byte[] buffer, int offset, int count)
    {
        WriteAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(LocalTcpStream));

        var data = new byte[count];
        Buffer.BlockCopy(buffer, offset, data, 0, count);

        using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        await _connection.SendAppDataAsync(data, combinedCts.Token);
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
