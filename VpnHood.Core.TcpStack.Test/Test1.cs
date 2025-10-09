using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using VpnHood.Core.Toolkit.Net;
using VpnHood.Core.VpnAdapters.Abstractions;
using VpnHood.Core.VpnAdapters.WinDivert;

namespace VpnHood.Core.TcpStack.Test;

[TestClass]
public sealed class TcpStackIntegrationTest
{
    private static readonly IPAddress TestServerIp = IPAddress.Parse("11.0.0.1");
    private const int TestServerPort = 8080;
    private const int TestDataSize = 10 * 1024 * 1024; // 10MB

    [TestMethod]
    public async Task TestTcpStackWithWinDivertAdapter_10MbEcho_ShouldSucceed()
    {
        // Arrange
        var testData = GenerateRandomTestData(TestDataSize);
        var tcpStack = new LocalTcpStack();
        var completionSource = new TaskCompletionSource<bool>();
        var receivedData = new List<byte>();
        
        var adapterSettings = new WinDivertVpnAdapterSettings
        {
            AdapterName = "VpnHoodTest",
            ExcludeLocalNetwork = false, // We want to capture local traffic for testing
            SimulateDns = false,
            // Required properties
            AutoDisposePackets = true,
            Blocking = true
        };
        
        using var adapter = new WinDivertVpnAdapter(adapterSettings);
        
        // Setup TCP stack integration with adapter's PacketReceived event
        adapter.PacketReceived += (sender, packet) =>
        {
            try
            {
                Console.WriteLine($"Packet received: {packet.SourceAddress}:{packet.DestinationAddress} Protocol: {packet.Protocol}");
                
                // Try to process with TCP stack
                Console.WriteLine(tcpStack.TryProcessPacket(packet.Buffer.Span)
                    ? "Packet processed by TCP stack"
                    : "Packet not handled by TCP stack");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing packet: {ex.Message}");
            }
        };
        
        // Setup TCP stack to send packets back through adapter
        tcpStack.OnPacketSend = packet =>
        {
            try
            {
                Console.WriteLine($"Sending packet back: {packet.SourceAddress}:{packet.DestinationAddress}");
                adapter.SendPacketQueued(packet);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error sending packet: {ex.Message}");
            }
        };
        
        // Setup echo server on our TCP stack
        var listener = tcpStack.Listen(new IPEndPoint(TestServerIp, TestServerPort));
        _ = StartEchoServer(listener, completionSource);

        try
        {
            // Configure and start adapter - let's try with minimal options first
            var options = new VpnAdapterOptions
            {
                SessionName = "TestSession",
                VirtualIpNetworkV4 = IpNetwork.Parse("10.0.0.0/24"),
                IncludeNetworks = [new IpNetwork(TestServerIp, 32)]
            };
            
            Console.WriteLine("Starting WinDivert adapter...");
            await adapter.Start(options, CancellationToken.None);
            
            // Wait a moment for everything to initialize
            await Task.Delay(2000);

            // Act - Connect with TcpClient and send/receive 10MB data
            using var tcpClient = new TcpClient();
            
            Console.WriteLine($"Connecting to {TestServerIp}:{TestServerPort}...");
            await tcpClient.ConnectAsync(TestServerIp, TestServerPort);
            Console.WriteLine("Connected successfully!");

            await using var stream = tcpClient.GetStream();
            
            // Send data in chunks and receive echo
            const int chunkSize = 8192;
            var sendTask = SendDataInChunks(stream, testData, chunkSize);
            var receiveTask = ReceiveDataInChunks(stream, receivedData, TestDataSize);

            // Wait for both send and receive to complete with timeout
            using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            await Task.WhenAll(sendTask, receiveTask).WaitAsync(cts.Token);
            
            // Signal completion
            completionSource.SetResult(true);

            // Assert
            Assert.AreEqual(TestDataSize, receivedData.Count, "Received data size should match sent data size");
            
            var receivedArray = receivedData.ToArray();
            CollectionAssert.AreEqual(testData, receivedArray, "Received data should match sent data exactly");

            Console.WriteLine($"✅ Test passed! Successfully echoed {TestDataSize:N0} bytes through TCP stack");
        }
        finally
        {
            try
            {
                Console.WriteLine("Stopping adapter...");
                adapter.Stop();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error stopping adapter: {ex.Message}");
            }
        }
    }

    private static Task StartEchoServer(LocalTcpListener listener, TaskCompletionSource<bool> completionSource)
    {
        return Task.Run(async () =>
        {
            try
            {
                Console.WriteLine("Echo server starting...");
                
                await foreach (var stream in listener.AcceptAllAsync())
                {
                    Console.WriteLine("New connection accepted by echo server");
                    
                    // Handle connection in background
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            var buffer = new byte[8192];
                            var totalEchoed = 0;
                            
                            while (true)
                            {
                                var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                                if (bytesRead == 0) break;
                                
                                // Echo the data back
                                await stream.WriteAsync(buffer, 0, bytesRead);
                                totalEchoed += bytesRead;
                                
                                if (totalEchoed % (1024 * 1024) == 0) // Log every MB
                                {
                                    Console.WriteLine($"Echoed {totalEchoed:N0} bytes so far...");
                                }
                            }
                            
                            Console.WriteLine($"Echo server finished. Total echoed: {totalEchoed:N0} bytes");
                            await stream.DisposeAsync();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Echo server connection error: {ex.Message}");
                        }
                    });
                    
                    // Break after handling first connection for this test
                    break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Echo server error: {ex.Message}");
                completionSource.SetException(ex);
            }
        });
    }

    private static async Task SendDataInChunks(NetworkStream stream, byte[] data, int chunkSize)
    {
        var totalSent = 0;
        
        for (var offset = 0; offset < data.Length; offset += chunkSize)
        {
            var currentChunkSize = Math.Min(chunkSize, data.Length - offset);
            await stream.WriteAsync(data, offset, currentChunkSize);
            totalSent += currentChunkSize;
            
            if (totalSent % (1024 * 1024) == 0) // Log every MB
            {
                Console.WriteLine($"Sent {totalSent:N0} bytes so far...");
            }
        }
        
        Console.WriteLine($"Finished sending {totalSent:N0} bytes");
    }

    private static async Task ReceiveDataInChunks(NetworkStream stream, List<byte> receivedData, int expectedSize)
    {
        var buffer = new byte[8192];
        var totalReceived = 0;
        
        while (totalReceived < expectedSize)
        {
            var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
            if (bytesRead == 0) break;
            
            receivedData.AddRange(buffer.Take(bytesRead));
            totalReceived += bytesRead;
            
            if (totalReceived % (1024 * 1024) == 0) // Log every MB
            {
                Console.WriteLine($"Received {totalReceived:N0} bytes so far...");
            }
        }
        
        Console.WriteLine($"Finished receiving {totalReceived:N0} bytes");
    }

    private static byte[] GenerateRandomTestData(int size)
    {
        var data = new byte[size];
        using var rng = RandomNumberGenerator.Create();
        rng.GetBytes(data);
        return data;
    }
}
