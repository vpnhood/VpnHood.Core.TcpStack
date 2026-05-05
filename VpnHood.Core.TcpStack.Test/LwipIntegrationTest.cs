using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using VpnHood.Core.TcpStack.LwIP;
using VpnHood.Core.Toolkit.Net;
using VpnHood.Core.VpnAdapters.Abstractions;
using VpnHood.Core.VpnAdapters.WinDivert;

namespace VpnHood.Core.TcpStack.Test;

[TestClass]
public sealed class LwipIntegrationTest
{
    private static readonly IPAddress TestServerIp = IPAddress.Parse("11.0.0.10");
    private const int TestServerPort = 8090;
    private const int TestDataSize = 256 * 1024; // 256 KB

    [TestMethod]
    [Timeout(90000)]
    public async Task LwipStack_WinDivert_Echo_ShouldSucceed()
    {
        var testData = GenerateRandomTestData(TestDataSize);
        var receivedData = new List<byte>();

        using var lwipStack = new LwipTcpStack();

        var adapterSettings = new WinDivertVpnAdapterSettings
        {
            AdapterName = "VpnHoodLwIP",
            ExcludeLocalNetwork = false,
            SimulateDns = false,
            AutoDisposePackets = true,
            Blocking = true,
        };

        using var adapter = new WinDivertVpnAdapter(adapterSettings);

        adapter.PacketReceived += (_, packet) =>
        {
            try
            {
                lwipStack.ProcessIncoming(packet.Buffer.Span);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ADAPTER] Error processing packet: {ex.Message}");
            }
        };

        lwipStack.OnPacketSend = packet =>
        {
            try
            {
                // ReSharper disable once AccessToDisposedClosure
                adapter.SendPacketQueued(packet);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[LWIP->ADAPTER] Error sending packet: {ex.Message}");
            }
        };

        lwipStack.ListenAny();

        _ = StartEchoServer(lwipStack);

        try
        {
            var options = new VpnAdapterOptions
            {
                SessionName = "LwipTestSession",
                VirtualIpNetworkV4 = IpNetwork.Parse("10.0.0.0/24"),
                IncludeNetworks = [new IpNetwork(TestServerIp, 32)]
            };

            Console.WriteLine("[TEST] Starting WinDivert adapter...");
            await adapter.Start(options, CancellationToken.None);
            Console.WriteLine("[TEST] Adapter started");

            // Allow WinDivert to settle after adapter start
            await Task.Delay(1000);

            using var tcpClient = new TcpClient { NoDelay = true };

            Console.WriteLine($"[TEST] Connecting to {TestServerIp}:{TestServerPort}...");
            using var connectCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await tcpClient.ConnectAsync(TestServerIp, TestServerPort, connectCts.Token);
            Console.WriteLine("[TEST] Connected!");

            await using var stream = tcpClient.GetStream();

            const int chunkSize = 8192;
            var sendTask = SendDataInChunks(stream, testData, chunkSize);
            var receiveTask = ReceiveDataInChunks(stream, receivedData, TestDataSize);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await Task.WhenAll(sendTask, receiveTask).WaitAsync(cts.Token);

            Assert.AreEqual(TestDataSize, receivedData.Count, "Received data size should match sent data size");
            CollectionAssert.AreEqual(testData, receivedData.ToArray(), "Received data should match sent data exactly");

            Console.WriteLine($"[TEST] Successfully echoed {TestDataSize:N0} bytes through LwIP TCP stack");
        }
        finally
        {
            Console.WriteLine("[TEST] Stopping adapter...");
            adapter.Stop();
        }
    }

    private static Task StartEchoServer(LwipTcpStack stack)
    {
        return Task.Run(async () =>
        {
            await foreach (var stream in stack.AcceptAllAsync())
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        var buffer = new byte[8192];
                        while (true)
                        {
                            var n = await stream.ReadAsync(buffer);
                            if (n == 0) break;
                            await stream.WriteAsync(buffer.AsMemory(0, n));
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ECHO SERVER] {ex.Message}");
                    }
                    finally
                    {
                        await stream.DisposeAsync();
                    }
                });
            }
        });
    }

    private static async Task SendDataInChunks(NetworkStream stream, byte[] data, int chunkSize)
    {
        var totalSent = 0;
        for (var offset = 0; offset < data.Length; offset += chunkSize)
        {
            var size = Math.Min(chunkSize, data.Length - offset);
            await stream.WriteAsync(data.AsMemory(offset, size));
            totalSent += size;
            if (totalSent % (32 * 1024) == 0)
                Console.WriteLine($"[CLIENT] Sent {totalSent:N0} bytes so far...");
        }
        Console.WriteLine($"[CLIENT] Finished sending {totalSent:N0} bytes");
    }

    private static async Task ReceiveDataInChunks(NetworkStream stream, List<byte> receivedData, int expectedSize)
    {
        var buffer = new byte[8192];
        while (receivedData.Count < expectedSize)
        {
            var n = await stream.ReadAsync(buffer);
            if (n == 0) break;
            receivedData.AddRange(buffer.Take(n));
            if (receivedData.Count % (32 * 1024) == 0)
                Console.WriteLine($"[CLIENT] Received {receivedData.Count:N0} bytes so far...");
        }
        Console.WriteLine($"[CLIENT] Finished receiving {receivedData.Count:N0} bytes");
    }

    private static byte[] GenerateRandomTestData(int size)
    {
        var data = new byte[size];
        RandomNumberGenerator.Fill(data);
        return data;
    }
}
