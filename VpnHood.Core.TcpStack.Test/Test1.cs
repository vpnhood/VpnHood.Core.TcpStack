using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using VpnHood.Core.Toolkit.Net;
using VpnHood.Core.VpnAdapters.WinDivert;

namespace VpnHood.Core.TcpStack.Test;

[TestClass]
public sealed class TcpStackIntegrationTest
{
    private const string TestServerIp = "11.0.0.1";
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
        
        using var adapter = new TestWinDivertAdapter(adapterSettings, tcpStack);
        
        // Setup echo server on our TCP stack
        var listener = tcpStack.Listen(new IPEndPoint(IPAddress.Parse(TestServerIp), TestServerPort));
        _ = StartEchoServer(listener, completionSource);

        try
        {
            // Configure and start adapter
            await adapter.StartAsync(CancellationToken.None);

            // Wait a moment for everything to initialize
            await Task.Delay(2000);

            // Act - Connect with TcpClient and send/receive 10MB data
            using var tcpClient = new TcpClient();
            
            Console.WriteLine($"Connecting to {TestServerIp}:{TestServerPort}...");
            await tcpClient.ConnectAsync(IPAddress.Parse(TestServerIp), TestServerPort);
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
                await adapter.StopAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error cleaning up adapter: {ex.Message}");
            }
        }
    }

    // Custom adapter wrapper that integrates with our TCP stack
    private class TestWinDivertAdapter : IDisposable
    {
        private readonly WinDivertVpnAdapter _adapter;
        private readonly LocalTcpStack _tcpStack;
        private readonly CancellationTokenSource _cts = new();
        private Task? _packetProcessingTask;

        public TestWinDivertAdapter(WinDivertVpnAdapterSettings settings, LocalTcpStack tcpStack)
        {
            _adapter = new WinDivertVpnAdapter(settings);
            _tcpStack = tcpStack;
            
            // Setup TCP stack integration
            _tcpStack.OnPacketSend = packet =>
            {
                try
                {
                    // Send packet back through adapter - need to access WritePacket somehow
                    // For now, we'll use reflection as a workaround
                    var writeMethod = typeof(WinDivertVpnAdapter).GetMethod("WritePacket", 
                        System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                    writeMethod?.Invoke(_adapter, [packet]);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error writing packet to adapter: {ex.Message}");
                }
            };
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            // Setup adapter with test route using reflection to access protected methods
            var addRouteMethod = typeof(WinDivertVpnAdapter).GetMethod("AddRoute", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var addAddressMethod = typeof(WinDivertVpnAdapter).GetMethod("AddAddress", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            
            await (Task)(addRouteMethod?.Invoke(_adapter, [IpNetwork.Parse("11.0.0.0/8"), cancellationToken]) ?? Task.CompletedTask);
            await (Task)(addAddressMethod?.Invoke(_adapter, [IpNetwork.Parse("11.0.0.1/24"), cancellationToken]) ?? Task.CompletedTask);
            
            // Start adapter (using protected methods via reflection for testing)
            var adapterAddMethod = typeof(WinDivertVpnAdapter).GetMethod("AdapterAdd", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var adapterOpenMethod = typeof(WinDivertVpnAdapter).GetMethod("AdapterOpen", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            
            await (Task)(adapterAddMethod?.Invoke(_adapter, [cancellationToken]) ?? Task.CompletedTask);
            await (Task)(adapterOpenMethod?.Invoke(_adapter, [cancellationToken]) ?? Task.CompletedTask);

            // Start packet processing
            _packetProcessingTask = ProcessPacketsAsync();
        }

        public async Task StopAsync()
        {
            await _cts.CancelAsync();
            
            if (_packetProcessingTask != null)
            {
                try
                {
                    await _packetProcessingTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            }

            // Stop adapter
            var adapterCloseMethod = typeof(WinDivertVpnAdapter).GetMethod("AdapterClose", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var adapterRemoveMethod = typeof(WinDivertVpnAdapter).GetMethod("AdapterRemove", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            
            adapterCloseMethod?.Invoke(_adapter, null);
            adapterRemoveMethod?.Invoke(_adapter, null);
        }

        private async Task ProcessPacketsAsync()
        {
            await Task.Run(() =>
            {
                var buffer = new byte[65536];
                var readMethod = typeof(WinDivertVpnAdapter).GetMethod("ReadPacket", 
                    System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

                try
                {
                    while (!_cts.Token.IsCancellationRequested)
                    {
                        // Read packet from adapter
                        var packetReceived = (bool)(readMethod?.Invoke(_adapter, [buffer]) ?? false);
                        
                        if (!packetReceived) 
                        {
                            Thread.Sleep(1);
                            continue;
                        }

                        Console.WriteLine("Packet received from adapter");

                        // Try to process with TCP stack first
                        if (_tcpStack.TryProcessPacket(buffer.AsSpan()))
                        {
                            Console.WriteLine("Packet processed by TCP stack");
                            continue;
                        }

                        Console.WriteLine("Packet not handled by TCP stack");
                    }
                }
                catch (Exception ex)
                {
                    if (!_cts.Token.IsCancellationRequested)
                        Console.WriteLine($"Packet processing error: {ex.Message}");
                }
            });
        }

        public void Dispose()
        {
            _cts.Cancel();
            _adapter.Dispose();
            _cts.Dispose();
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
