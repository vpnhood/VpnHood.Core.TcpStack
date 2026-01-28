using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;
using VpnHood.Core.Toolkit.Net;
using VpnHood.Core.VpnAdapters.Abstractions;
using VpnHood.Core.VpnAdapters.WinDivert;

namespace VpnHood.Core.TcpStack.Test;

[TestClass]
public sealed class TcpStackIntegrationTest
{
    private static readonly IPAddress TestServerIp = IPAddress.Parse("11.0.0.1");
    private const int TestServerPort = 8080;
    private const int TestDataSize = 100 * 1024; // 100KB for faster testing

    [TestMethod]
    [Timeout(30000)] // 30 seconds timeout
    public async Task TestTcpStackWithWinDivertAdapter_Echo_ShouldSucceed()
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
            Blocking = true,
        };
        
        using var adapter = new WinDivertVpnAdapter(adapterSettings);

        var packetCount = 0;
        var tcpPacketCount = 0;
        
        // Setup TCP stack integration with adapter's PacketReceived event
        adapter.PacketReceived += (sender, packet) =>
        {
            try
            {
                packetCount++;
                if (packet.Protocol == IpProtocol.Tcp)
                {
                    tcpPacketCount++;
                    var tcp = packet.ExtractTcp();
                    var payloadLen = tcp.Payload.Length;
                    Console.WriteLine($"[ADAPTER] TCP Packet #{tcpPacketCount}: {packet.SourceAddress}:{tcp.SourcePort} -> {packet.DestinationAddress}:{tcp.DestinationPort}, SYN={tcp.Synchronize}, ACK={tcp.Acknowledgment}, PayloadLen={payloadLen}");
                }
                else
                {
                    Console.WriteLine($"[ADAPTER] Packet #{packetCount} received: {packet.SourceAddress}:{packet.DestinationAddress} Protocol: {packet.Protocol}, Length: {packet.Buffer.Length}");
                }
                
                // Process with TCP stack
                tcpStack.ProcessIncoming(packet.Buffer.Span);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ADAPTER] Error processing packet: {ex.Message}");
                Console.WriteLine($"[ADAPTER] Stack trace: {ex.StackTrace}");
            }
        };
        
        // Setup TCP stack to send packets back through adapter
        tcpStack.OnPacketSend = packet =>
        {
            try
            {
                if (packet.Protocol == IpProtocol.Tcp)
                {
                    var tcp = packet.ExtractTcp();
                    Console.WriteLine($"[TCP STACK -> ADAPTER] Sending TCP packet: {packet.SourceAddress}:{tcp.SourcePort} -> {packet.DestinationAddress}:{tcp.DestinationPort}, PayloadLen={tcp.Payload.Length}");
                }
                else
                {
                    Console.WriteLine($"[TCP STACK -> ADAPTER] Sending packet: {packet.SourceAddress} -> {packet.DestinationAddress}");
                }
                adapter.SendPacketQueued(packet);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[TCP STACK -> ADAPTER] Error sending packet: {ex.Message}");
            }
        };
        
        // Setup echo server on our TCP stack
        var listener = tcpStack.Listen(new IPEndPoint(TestServerIp, TestServerPort));
        Console.WriteLine($"[TEST] Echo server listener created on {TestServerIp}:{TestServerPort}");
        
        _ = StartEchoServer(listener, completionSource);

        try
        {
            // Configure and start adapter
            var options = new VpnAdapterOptions
            {
                SessionName = "TestSession",
                VirtualIpNetworkV4 = IpNetwork.Parse("10.0.0.0/24"),
                IncludeNetworks = [new IpNetwork(TestServerIp, 32)]
            };
            
            Console.WriteLine("[TEST] Starting WinDivert adapter...");
            Console.WriteLine($"[TEST] Include networks: {string.Join<IpNetwork>(", ", options.IncludeNetworks)}");
            await adapter.Start(options, CancellationToken.None);
            Console.WriteLine("[TEST] Adapter started successfully");
            
            // Act - Connect with TcpClient and send/receive data
            using var tcpClient = new TcpClient();
            
            Console.WriteLine($"[TEST] Connecting to {TestServerIp}:{TestServerPort}...");
            
            using var connectCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await tcpClient.ConnectAsync(TestServerIp, TestServerPort, connectCts.Token);
            Console.WriteLine("[TEST] Connected successfully!");

            await using var stream = tcpClient.GetStream();
            
            // Send data in chunks and receive echo
            const int chunkSize = 8192;
            var sendTask = SendDataInChunks(stream, testData, chunkSize);
            var receiveTask = ReceiveDataInChunks(stream, receivedData, TestDataSize);

            // Wait for both send and receive to complete with timeout
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await Task.WhenAll(sendTask, receiveTask).WaitAsync(cts.Token);
            
            // Signal completion
            completionSource.SetResult(true);

            // Assert
            Assert.AreEqual(TestDataSize, receivedData.Count, "Received data size should match sent data size");
            
            var receivedArray = receivedData.ToArray();
            CollectionAssert.AreEqual(testData, receivedArray, "Received data should match sent data exactly");

            Console.WriteLine($"[TEST] ✅ Test passed! Successfully echoed {TestDataSize:N0} bytes through TCP stack");
            Console.WriteLine($"[TEST] Total packets received: {packetCount}, TCP packets: {tcpPacketCount}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TEST] ❌ Test failed with exception: {ex.Message}");
            Console.WriteLine($"[TEST] Stack trace: {ex.StackTrace}");
            throw;
        }
        finally
        {
            try
            {
                Console.WriteLine("[TEST] Stopping adapter...");
                adapter.Stop();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[TEST] Error stopping adapter: {ex.Message}");
            }
        }
    }

    private static Task StartEchoServer(LocalTcpListener listener, TaskCompletionSource<bool> completionSource)
    {
        return Task.Run(async () =>
        {
            try
            {
                Console.WriteLine("[ECHO SERVER] Starting...");
                Console.WriteLine($"[ECHO SERVER] Listening on {listener.LocalEndPoint}");
                
                await foreach (var stream in listener.AcceptAllAsync())
                {
                    Console.WriteLine("[ECHO SERVER] ✅ New connection accepted!");
                    
                    // Handle connection in background
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            Console.WriteLine("[ECHO SERVER] Connection handler started");
                            var buffer = new byte[8192];
                            var totalEchoed = 0;
                            
                            while (true)
                            {
                                Console.WriteLine("[ECHO SERVER] Calling ReadAsync...");
                                var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                                Console.WriteLine($"[ECHO SERVER] ReadAsync returned {bytesRead} bytes");
                                if (bytesRead == 0) break;
                                
                                // Echo the data back
                                Console.WriteLine($"[ECHO SERVER] Echoing {bytesRead} bytes back...");
                                await stream.WriteAsync(buffer, 0, bytesRead);
                                totalEchoed += bytesRead;
                                
                                if (totalEchoed % 10240 == 0) // Log every 10KB
                                {
                                    Console.WriteLine($"[ECHO SERVER] Echoed {totalEchoed:N0} bytes so far...");
                                }
                            }
                            
                            Console.WriteLine($"[ECHO SERVER] Connection finished. Total echoed: {totalEchoed:N0} bytes");
                            await stream.DisposeAsync();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ECHO SERVER] Connection error: {ex.Message}");
                            Console.WriteLine($"[ECHO SERVER] Stack trace: {ex.StackTrace}");
                        }
                    });
                    
                    // Break after handling first connection for this test
                    break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ECHO SERVER] Error: {ex.Message}");
                Console.WriteLine($"[ECHO SERVER] Stack trace: {ex.StackTrace}");
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
            
            if (totalSent % 10240 == 0) // Log every 10KB
            {
                Console.WriteLine($"[CLIENT] Sent {totalSent:N0} bytes so far...");
            }
        }
        
        Console.WriteLine($"[CLIENT] Finished sending {totalSent:N0} bytes");
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
            
            if (totalReceived % 10240 == 0) // Log every 10KB
            {
                Console.WriteLine($"[CLIENT] Received {totalReceived:N0} bytes so far...");
            }
        }
        
        Console.WriteLine($"[CLIENT] Finished receiving {totalReceived:N0} bytes");
    }

    private static byte[] GenerateRandomTestData(int size)
    {
        var data = new byte[size];
        using var rng = RandomNumberGenerator.Create();
        rng.GetBytes(data);
        return data;
    }
}
