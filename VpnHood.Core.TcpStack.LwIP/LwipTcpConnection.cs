using System.IO.Pipelines;
using System.Net;

namespace VpnHood.Core.TcpStack.LwIP;

/// <summary>
/// Represents a single TCP connection managed by the lwIP stack.
/// Bridges between lwIP callbacks and the pipe-based stream.
/// </summary>
internal sealed class LwipTcpConnection : IDisposable
{
    private readonly Pipe _receivePipe = new(new PipeOptions(
        pauseWriterThreshold: 256 * 1024,
        resumeWriterThreshold: 128 * 1024,
        useSynchronizationContext: false));

    private readonly SemaphoreSlim _sendBufferSignal = new(0, 1);
    private readonly nint _conn;
    private readonly LwipTcpStack _stack;
    private bool _disposed;
    private bool _remoteClosed;

    public IPEndPoint LocalEndPoint { get; }
    public IPEndPoint RemoteEndPoint { get; }
    public PipeReader ReceiveReader => _receivePipe.Reader;

    internal LwipTcpConnection(nint conn, LwipTcpStack stack, IPEndPoint localEp, IPEndPoint remoteEp)
    {
        _conn = conn;
        _stack = stack;
        LocalEndPoint = localEp;
        RemoteEndPoint = remoteEp;
    }

    /// <summary>
    /// Called from the native receive callback. Writes received data into the pipe.
    /// Returns the number of bytes consumed (acknowledged to lwIP).
    /// </summary>
    internal int OnDataReceived(ReadOnlySpan<byte> data)
    {
        if (_disposed || _remoteClosed) return data.Length;

        var writer = _receivePipe.Writer;
        var span = writer.GetSpan(data.Length);
        data.CopyTo(span);
        writer.Advance(data.Length);

        // FlushAsync on an in-memory pipe is always synchronous
        _ = writer.FlushAsync();

        return data.Length;
    }

    /// <summary>
    /// Called when the remote side closes the connection.
    /// </summary>
    internal void OnRemoteClosed(int err)
    {
        _remoteClosed = true;
        try { _receivePipe.Writer.Complete(); } catch { /* already completed */ }
        TrySignalSendBuffer();
    }

    /// <summary>
    /// Called when lwIP acknowledges sent data (send buffer space freed).
    /// </summary>
    internal void OnSendBufferAvailable(int len)
    {
        TrySignalSendBuffer();
    }

    /// <summary>
    /// Writes data to the connection, waiting for send buffer space if needed.
    /// </summary>
    public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        if (_disposed || _remoteClosed) return;

        var remaining = data;
        while (remaining.Length > 0 && !_disposed && !_remoteClosed) {
            var sendBufferSpace = _stack.GetSendBufferSpace(_conn);
            if (sendBufferSpace <= 0) {
                // Wait for send buffer to become available
                DrainSendBufferSignal();
                try { await _sendBufferSignal.WaitAsync(ct); }
                catch (OperationCanceledException) { return; }
                continue;
            }

            var toWrite = Math.Min(remaining.Length, sendBufferSpace);
            var written = _stack.Write(_conn, remaining.Span[..toWrite]);
            if (written <= 0) {
                // Wait and retry
                DrainSendBufferSignal();
                try { await _sendBufferSignal.WaitAsync(ct); }
                catch (OperationCanceledException) { return; }
                continue;
            }

            remaining = remaining[written..];
        }
    }

    /// <summary>
    /// Gracefully closes the TCP connection.
    /// </summary>
    public void Close()
    {
        if (_disposed) return;
        _stack.CloseConnection(_conn);
        try { _receivePipe.Writer.Complete(); } catch { /* already completed */ }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try { _receivePipe.Writer.Complete(); } catch { /* ignore */ }
        TrySignalSendBuffer();
    }

    private void TrySignalSendBuffer()
    {
        if (_sendBufferSignal.CurrentCount == 0) {
            try { _sendBufferSignal.Release(); }
            catch (SemaphoreFullException) { /* already signalled */ }
        }
    }

    private void DrainSendBufferSignal()
    {
        while (_sendBufferSignal.Wait(0)) { }
    }
}
