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
    private const int TestDataSizeMb = 100;
    private const bool UseFixedWindow = true;
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

        var notification = new Notification.Builder(this, channelId)
            .SetContentTitle("TcpStack Test")
            .SetSmallIcon(Android.Resource.Drawable.IcMenuCompass)
            .Build();

        if (OperatingSystem.IsAndroidVersionAtLeast(29))
            StartForeground(1, notification, ForegroundService.TypeSystemExempted);
        else
            StartForeground(1, notification);

        Task.Run(RunEchoTest);
        return StartCommandResult.NotSticky;
    }

    private void Log(string msg)
    {
        var line = $"[{DateTime.Now:HH:mm:ss.fff}] {msg}";
        Android.Util.Log.Info("TcpStackTest", line);
        OnLog?.Invoke(line);
    }

    private async Task RunEchoTest()
    {
        Log("=== ECHO TEST START ===");
        Log($"Data size: {TestDataSizeMb} MB, Server: {TestServerIp}:{TestServerPort}");

        var adapterSettings = new AndroidVpnAdapterSettings
        {
            AdapterName = "TcpStackTest",
            AutoDisposePackets = true,
            Blocking = true,
        };
        using var adapter = new AndroidVpnAdapter(this, adapterSettings);

        var tcpStack = new LocalTcpStack
        {
            UseFixedSendWindow = UseFixedWindow
        };
        LocalTcpStack.DiagLog = msg => Log(msg);
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

        // Echo server
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var stream in listener.AcceptAllAsync())
                {
                    Log("[SERVER] Connection accepted!");
                    var buf = new byte[65536];
                    using var ms = new MemoryStream();
                    while (true)
                    {
                        var n = await stream.Stream.ReadAsync(buf, 0, buf.Length);
                        if (n == 0) break;
                        ms.Write(buf, 0, n);
                    }
                    Log($"[SERVER] Received {ms.Length:N0} bytes. Echoing...");
                    ms.Position = 0;
                    var serverSentBytes = 0;
                    var serverNextLog = 2 * 1024 * 1024;
                    var serverSendBuf = new byte[65536];
                    var serverSendSw = Stopwatch.StartNew();
                    while (true)
                    {
                        var n = await ms.ReadAsync(serverSendBuf.AsMemory());
                        if (n == 0) break;
                        await stream.Stream.WriteAsync(serverSendBuf.AsMemory(0, n));
                        serverSentBytes += n;
                        if (serverSentBytes >= serverNextLog)
                        {
                            Log($"[SERVER] Echoed {serverSentBytes / (1024 * 1024)} MB ({serverSentBytes * 8.0 / serverSendSw.Elapsed.TotalSeconds / 1_000_000:F1} Mbit/s)");
                            serverNextLog += 2 * 1024 * 1024;
                        }
                    }
                    Log($"[SERVER] Echo complete: {serverSentBytes:N0} bytes in {serverSendSw.Elapsed.TotalSeconds:F2}s ({serverSentBytes * 8.0 / serverSendSw.Elapsed.TotalSeconds / 1_000_000:F1} Mbit/s)");
                    await stream.DisposeAsync();
                    break;
                }
            }
            catch (Exception ex) { Log($"[SERVER] Error: {ex.Message}"); }
        });

        // Start TUN adapter
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

            // Generate test data
            var testData = new byte[TestDataSize];
            for (var i = 0; i < testData.Length; i++) testData[i] = (byte)(i % 251);

            Log($"Connecting to {TestServerIp}:{TestServerPort}...");
            using var tcpClient = new TcpClient();
            tcpClient.NoDelay = true;

            using var connectCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await tcpClient.ConnectAsync(TestServerIp, TestServerPort, connectCts.Token);
            Log("Connected!");

            await using var stream = tcpClient.GetStream();

            var sw = Stopwatch.StartNew();
            var receivedData = new byte[TestDataSize];
            var receiveTask = ReceiveAll(stream, receivedData, TestDataSize);

            // Send data
            Log("Sending data...");
            var sendSw = Stopwatch.StartNew();
            const int chunkSize = 65536;
            for (var offset = 0; offset < testData.Length; offset += chunkSize)
            {
                var len = Math.Min(chunkSize, testData.Length - offset);
                await stream.WriteAsync(testData.AsMemory(offset, len), CancellationToken.None);
                if (offset > 0 && offset % (5 * 1024 * 1024) == 0)
                    Log($"  Sent {offset / (1024 * 1024)} MB ({offset * 8.0 / sendSw.Elapsed.TotalSeconds / 1_000_000:F1} Mbit/s)");
            }
            sendSw.Stop();
            Log($"Send complete: {testData.Length:N0} bytes in {sendSw.Elapsed.TotalSeconds:F2}s " +
                $"({testData.Length * 8.0 / sendSw.Elapsed.TotalSeconds / 1_000_000:F1} Mbit/s)");

            tcpClient.Client.Shutdown(SocketShutdown.Send);

            Log("Waiting for echo...");
            using var echoCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            var totalReceived = await receiveTask.WaitAsync(echoCts.Token);
            sw.Stop();

            Log($"Received: {totalReceived:N0} bytes in {sw.Elapsed.TotalSeconds:F2}s");
            Log($"Download throughput: {totalReceived * 8.0 / sw.Elapsed.TotalSeconds / 1_000_000:F1} Mbit/s");
            Log($"Packets total={packetCount}, tcp={tcpPacketCount}");

            if (totalReceived == TestDataSize && testData.AsSpan().SequenceEqual(receivedData.AsSpan(0, totalReceived)))
                Log("=== TEST PASSED ===");
            else
                Log($"=== TEST FAILED === received={totalReceived}, expected={TestDataSize}");
        }
        catch (Exception ex)
        {
            Log($"ERROR: {ex.GetType().Name}: {ex.Message}");
            Log($"Stack: {ex.StackTrace}");
        }
        finally
        {
            Log("Stopping adapter...");
            adapter.Stop();
            StopSelf();
        }
    }

    private async Task<int> ReceiveAll(NetworkStream stream, byte[] buffer, int expectedSize)
    {
        var readBuf = new byte[65536];
        var total = 0;
        var sw = Stopwatch.StartNew();
        var nextLogMb = 5;
        var lastLogTime = sw.Elapsed;
        while (total < expectedSize)
        {
            using var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            int n;
            try
            {
                n = await stream.ReadAsync(readBuf.AsMemory(0, Math.Min(readBuf.Length, expectedSize - total)), readCts.Token);
            }
            catch (OperationCanceledException)
            {
                Log($"  STALL: received {total / 1024} KB in {sw.Elapsed.TotalSeconds:F1}s ({total * 8.0 / Math.Max(sw.Elapsed.TotalSeconds, 0.001) / 1_000_000:F1} Mbit/s), no data for 5s");
                continue;
            }
            if (n == 0) break;
            Buffer.BlockCopy(readBuf, 0, buffer, total, n);
            total += n;
            if (total / (1024 * 1024) >= nextLogMb)
            {
                Log($"  Recv {total / (1024 * 1024)} MB ({total * 8.0 / sw.Elapsed.TotalSeconds / 1_000_000:F1} Mbit/s)");
                nextLogMb += 5;
            }
            else if (sw.Elapsed - lastLogTime > TimeSpan.FromSeconds(10))
            {
                Log($"  Recv progress: {total / 1024} KB in {sw.Elapsed.TotalSeconds:F1}s");
                lastLogTime = sw.Elapsed;
            }
        }
        return total;
    }
}
