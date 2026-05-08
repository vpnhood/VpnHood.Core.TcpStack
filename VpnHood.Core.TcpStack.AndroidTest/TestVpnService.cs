using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using Android;
using Android.App;
using Android.Content;
using Android.Content.PM;
using Android.Net;
using Android.Runtime;
using VpnHood.Core.Packets;
using VpnHood.Core.Toolkit.Net;
using VpnHood.Core.VpnAdapters.Abstractions;
using VpnHood.Core.VpnAdapters.AndroidTun;

namespace VpnHood.Core.TcpStack.AndroidTest;

[Service(
    Permission = Manifest.Permission.BindVpnService,
    Exported = false,
    ForegroundServiceType = ForegroundService.TypeSystemExempted)]
[IntentFilter(["android.net.VpnService"])]
public class TestVpnService : VpnService
{
    private static readonly IPAddress TestServerIp = IPAddress.Parse("11.0.0.1");
    private const int TestServerPort = 8080;
    private const int TestDataSizeMb = 200;
    private const bool UseFixedWindow = false;
    private const int WorkerCount = 1;
    private const int StallTimeoutSeconds = 30;
    private const int TestDataSize = TestDataSizeMb * 1024 * 1024;

    // Static log callback so the Activity can display lines
    public static event Action<string>? OnLog;

    [return: GeneratedEnum]
    public override StartCommandResult OnStartCommand(Intent? intent, [GeneratedEnum] StartCommandFlags flags, int startId)
    {
        // Must call startForeground quickly to avoid ANR on Android 14+
        const string channelId = "tcpstack_test";
        if (OperatingSystem.IsAndroidVersionAtLeast(26))
        {
            var channel = new NotificationChannel(channelId, "TcpStack Test", NotificationImportance.Low);
            var nm = (NotificationManager?)GetSystemService(NotificationService);
            nm?.CreateNotificationChannel(channel);
        }

        if (!OperatingSystem.IsAndroidVersionAtLeast(34))
            throw new NotSupportedException("Requires Android 14+ for foreground service without notification");

        var notification = new Notification.Builder(this, channelId)
            .SetContentTitle("TcpStack Test")
            .SetSmallIcon(Android.Resource.Drawable.IcMenuCompass)
            .Build();

        StartForeground(1, notification, ForegroundService.TypeSystemExempted);

        Task.Run(RunEchoTest);
        return StartCommandResult.NotSticky;
    }

    // Status messages: system log + UI window
    private void Log(string msg)
    {
        var line = $"[{DateTime.Now:HH:mm:ss.fff}] {msg}";
        Android.Util.Log.Info("TcpStackTest", line);
        OnLog?.Invoke(line);
    }

    // Detailed/diagnostic messages: system log only
    private void LogSystem(string msg)
    {
        Android.Util.Log.Info("TcpStackTest", $"[{DateTime.Now:HH:mm:ss.fff}] {msg}");
    }

    private async Task RunEchoTest()
    {
        Log($"=== ECHO TEST START: {WorkerCount} workers x {TestDataSizeMb} MB ===");

        var adapterSettings = new AndroidVpnAdapterSettings
        {
            AdapterName = "TcpStackTest",
            AutoDisposePackets = true,
            Blocking = true,
        };
        using var adapter = new AndroidVpnAdapter(this, adapterSettings);

        var tcpStack = new LocalTcpStack { UseFixedSendWindow = UseFixedWindow };
        LocalTcpStack.DiagLog = msg => LogSystem(msg);
        var packetCount = 0;
        var tcpPacketCount = 0;

        adapter.PacketReceived += (_, packet) =>
        {
            packetCount++;
            if (packet.Protocol == IpProtocol.Tcp) tcpPacketCount++;
            tcpStack.ProcessIncoming(packet.Buffer.Span);
        };

        tcpStack.OnPacketSend = packet => adapter.SendPacketQueued(packet);

        var listener = tcpStack.Listen(new IpEndPointValue(TestServerIp, TestServerPort));

        using var serverCts = new CancellationTokenSource();

        // Echo server: accept all connections and handle each concurrently
        var serverTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var conn in listener.AcceptAllAsync(serverCts.Token))
                {
                    var capturedConn = conn;
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            var buf = new byte[65536];
                            using var ms = new MemoryStream();
                            while (true)
                            {
                                var n = await capturedConn.Stream.ReadAsync(buf, 0, buf.Length);
                                if (n == 0) break;
                                ms.Write(buf, 0, n);
                            }
                            LogSystem($"[SERVER] Received {ms.Length:N0} bytes. Echoing...");
                            ms.Position = 0;
                            var sendBuf = new byte[65536];
                            var sent = 0;
                            var echoSw = Stopwatch.StartNew();
                            while (true)
                            {
                                var n = await ms.ReadAsync(sendBuf.AsMemory());
                                if (n == 0) break;
                                await capturedConn.Stream.WriteAsync(sendBuf.AsMemory(0, n));
                                sent += n;
                            }
                            LogSystem($"[SERVER] Echo done: {sent:N0} bytes in {echoSw.Elapsed.TotalSeconds:F2}s ({sent * 8.0 / echoSw.Elapsed.TotalSeconds / 1_000_000:F1} Mbit/s)");
                            await capturedConn.DisposeAsync();
                        }
                        catch (Exception ex) { LogSystem($"[SERVER] conn error: {ex.Message}"); }
                    });
                }
            }
            catch (OperationCanceledException) { /* expected on shutdown */ }
            catch (Exception ex) { Log($"[SERVER] Accept error: {ex.Message}"); }
        });

        var options = new VpnAdapterOptions
        {
            SessionName = "TcpStackTest",
            VirtualIpNetworkV4 = IpNetwork.Parse("10.0.0.1/24"),
            IncludeNetworks = [new IpNetwork(TestServerIp, 32)],
            DnsServers = [IPAddress.Parse("8.8.8.8")],
        };

        try
        {
            Log("Starting TUN adapter...");
            await adapter.Start(options, CancellationToken.None);
            Log("TUN adapter started.");
            await Task.Delay(200);

            var testData = new byte[TestDataSize];
            for (var i = 0; i < testData.Length; i++) testData[i] = (byte)(i % 251);

            Log($"Launching {WorkerCount} workers...");
            var totalSw = Stopwatch.StartNew();

            var workerTasks = Enumerable.Range(1, WorkerCount)
                .Select(id => RunWorker(id, testData))
                .ToArray();

            var results = await Task.WhenAll(workerTasks);
            totalSw.Stop();

            var passed = results.Count(r => r);
            Log($"Packets total={packetCount}, tcp={tcpPacketCount}");

            if (passed == WorkerCount)
                Log($"=== TEST PASSED: {passed}/{WorkerCount} workers in {totalSw.Elapsed.TotalSeconds:F2}s ===");
            else
                Log($"=== TEST FAILED: {passed}/{WorkerCount} workers passed in {totalSw.Elapsed.TotalSeconds:F2}s ===");
        }
        catch (Exception ex)
        {
            Log($"ERROR: {ex.GetType().Name}: {ex.Message}");
            Log($"Stack: {ex.StackTrace}");
        }
        finally
        {
            serverCts.Cancel();
            Log("Stopping adapter...");
            adapter.Stop();
            StopSelf();
        }
    }

    private async Task<bool> RunWorker(int id, byte[] testData)
    {
        var tag = $"[W{id}]";
        try
        {
            Log($"{tag} Connecting...");
            using var tcpClient = new TcpClient { NoDelay = true };
            using var connectCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await tcpClient.ConnectAsync(TestServerIp, TestServerPort, connectCts.Token);
            Log($"{tag} Connected!");

            await using var stream = tcpClient.GetStream();

            // Send all data first (echo server buffers everything before echoing back)
            Log($"{tag} Sending {testData.Length / (1024 * 1024)} MB...");
            var sendSw = Stopwatch.StartNew();
            const int chunkSize = 65536;
            for (var offset = 0; offset < testData.Length; offset += chunkSize)
            {
                var len = Math.Min(chunkSize, testData.Length - offset);
                await stream.WriteAsync(testData.AsMemory(offset, len), CancellationToken.None);
                if (offset > 0 && offset % (5 * 1024 * 1024) == 0)
                    Log($"{tag} Sent {offset / (1024 * 1024)} MB ({offset * 8.0 / sendSw.Elapsed.TotalSeconds / 1_000_000:F1} Mbit/s)");
            }
            sendSw.Stop();
            Log($"{tag} Send done: {testData.Length / (1024 * 1024)} MB in {sendSw.Elapsed.TotalSeconds:F2}s ({testData.Length * 8.0 / sendSw.Elapsed.TotalSeconds / 1_000_000:F1} Mbit/s)");

            tcpClient.Client.Shutdown(SocketShutdown.Send);

            // Now receive the echoed data
            Log($"{tag} Receiving echo...");
            var receivedData = new byte[TestDataSize];
            using var echoCts = new CancellationTokenSource(TimeSpan.FromSeconds(StallTimeoutSeconds));
            var totalReceived = await ReceiveAll(tag, stream, receivedData, TestDataSize, echoCts.Token);

            if (totalReceived == TestDataSize && testData.AsSpan().SequenceEqual(receivedData.AsSpan(0, totalReceived)))
            {
                Log($"{tag} PASSED ({totalReceived:N0} bytes)");
                return true;
            }

            Log($"{tag} FAILED: received={totalReceived}, expected={TestDataSize}");
            return false;
        }
        catch (Exception ex)
        {
            Log($"{tag} ERROR: {ex.GetType().Name}: {ex.Message}");
            return false;
        }
    }

    private async Task<int> ReceiveAll(string tag, NetworkStream stream, byte[] buffer, int expectedSize, CancellationToken ct)
    {
        var readBuf = new byte[65536];
        var total = 0;
        var stallCount = 0;
        var sw = Stopwatch.StartNew();
        var nextLogMb = 5;
        while (total < expectedSize)
        {
            ct.ThrowIfCancellationRequested();
            using var readCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            readCts.CancelAfter(TimeSpan.FromSeconds(5));
            int n;
            try
            {
                n = await stream.ReadAsync(readBuf.AsMemory(0, Math.Min(readBuf.Length, expectedSize - total)), readCts.Token);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                stallCount++;
                Log($"{tag} STALL #{stallCount}: {total / 1024} KB received after {sw.Elapsed.TotalSeconds:F1}s");
                continue;
            }
            if (n == 0) break;
            stallCount = 0;
            Buffer.BlockCopy(readBuf, 0, buffer, total, n);
            total += n;
            if (total / (1024 * 1024) >= nextLogMb)
            {
                Log($"{tag} Recv {total / (1024 * 1024)} MB ({total * 8.0 / sw.Elapsed.TotalSeconds / 1_000_000:F1} Mbit/s)");
                nextLogMb += 5;
            }
        }
        return total;
    }
}
