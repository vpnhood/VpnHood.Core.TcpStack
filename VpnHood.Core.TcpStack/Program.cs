using System.Net;
using System.Text;
using VpnHood.Core.TcpStack;

namespace VpnHood.Core.TcpStack;

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("LocalTcpStack Demo - Lightweight localhost-only TCP stack");
        Console.WriteLine("=========================================================");
        
        var tcpStack = new LocalTcpStack();
        
        // Set up integration with a mock adapter
        tcpStack.IntegrateWithAdapter(packetBytes =>
        {
            Console.WriteLine($"[ADAPTER] Sending packet back to network: {packetBytes.Length} bytes");
            // In real implementation, this would call adapter.WritePacket(packet)
        });
        
        // Listen on localhost:8080
        var listener = tcpStack.Listen(new IPEndPoint(IPAddress.Loopback, 8080));
        Console.WriteLine("TCP Stack listening on 127.0.0.1:8080");
        
        // Start echo server
        _ = Task.Run(async () =>
        {
            Console.WriteLine("Starting echo server...");
            await foreach (var stream in listener.AcceptAllAsync())
            {
                Console.WriteLine("New connection accepted");
                
                // Handle each connection as an echo server
                _ = Task.Run(async () =>
                {
                    try
                    {
                        var buffer = new byte[1024];
                        int bytesRead;
                        
                        while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                        {
                            var message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                            Console.WriteLine($"[ECHO] Received: {message.Trim()}");
                            
                            // Echo back
                            await stream.WriteAsync(buffer, 0, bytesRead);
                            Console.WriteLine($"[ECHO] Sent back: {message.Trim()}");
                        }
                        
                        Console.WriteLine("Connection closed by client");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERROR] Connection error: {ex.Message}");
                    }
                    finally
                    {
                        stream.Dispose();
                    }
                });
            }
        });
        
        Console.WriteLine("\nIntegration Guide:");
        Console.WriteLine("==================");
        Console.WriteLine("1. In your TunVpnAdapter ReadPacket override:");
        Console.WriteLine("   if (tcpStack.TryProcessPacket(packetData))");
        Console.WriteLine("       return true; // Packet handled by TCP stack");
        Console.WriteLine();
        Console.WriteLine("2. In your adapter setup:");
        Console.WriteLine("   tcpStack.IntegrateWithAdapter(packetBytes => WritePacket(IpPacket.Parse(packetBytes)));");
        Console.WriteLine();
        Console.WriteLine("3. Start your TCP listeners:");
        Console.WriteLine("   var listener = tcpStack.Listen(new IPEndPoint(IPAddress.Loopback, port));");
        Console.WriteLine("   await foreach (var stream in listener.AcceptAllAsync())");
        Console.WriteLine("   {");
        Console.WriteLine("       // Handle connection with regular Stream API");
        Console.WriteLine("   }");
        Console.WriteLine();
        Console.WriteLine("Features:");
        Console.WriteLine("- Uses .NET Channels for async data flow");
        Console.WriteLine("- Provides standard Stream interface (LocalTcpStream)");
        Console.WriteLine("- Supports simultaneous connections");
        Console.WriteLine("- Lightweight, localhost-only (no congestion control)");
        Console.WriteLine("- Integrates with existing VpnHood.Core.Packets");
        Console.WriteLine();
        Console.WriteLine("Press Ctrl+C to exit");
        
        // Keep the program running
        try
        {
            await Task.Delay(-1);
        }
        catch (TaskCanceledException)
        {
            Console.WriteLine("\nShutting down...");
        }
    }
}
