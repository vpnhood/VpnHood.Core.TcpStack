using System.Net;

namespace VpnHood.Core.TcpStack;

/// <summary>
/// Integration helper for connecting LocalTcpStack with TunVpnAdapter
/// </summary>
public static class TcpStackIntegration
{
    /// <summary>
    /// Integrates a LocalTcpStack with a TunVpnAdapter for localhost TCP handling
    /// </summary>
    /// <param name="tcpStack">The TCP stack to integrate</param>
    /// <param name="writePacketCallback">Callback to write packets back to the adapter</param>
    /// <param name="isLocalhost">Function to determine if an address is localhost</param>
    public static void IntegrateWithAdapter(
        this LocalTcpStack tcpStack, 
        Action<byte[]> writePacketCallback,
        Func<IPAddress, bool>? isLocalhost = null)
    {
        isLocalhost ??= addr => IPAddress.IsLoopback(addr);
        
        // Set up packet send callback
        tcpStack.OnPacketSend = packet =>
        {
            var buffer = packet.Buffer.ToArray();
            writePacketCallback(buffer);
        };
    }
    
    /// <summary>
    /// Processes an incoming packet through the TCP stack if it's a localhost TCP packet
    /// </summary>
    /// <param name="tcpStack">The TCP stack</param>
    /// <param name="packetData">Raw packet data</param>
    /// <param name="isLocalhost">Function to determine if an address is localhost</param>
    /// <returns>True if the packet was handled by the TCP stack</returns>
    public static bool TryProcessPacket(
        this LocalTcpStack tcpStack,
        ReadOnlySpan<byte> packetData,
        Func<IPAddress, bool>? isLocalhost = null)
    {
        try
        {
            isLocalhost ??= addr => IPAddress.IsLoopback(addr);
            
            // Quick check if this might be a TCP packet
            if (packetData.Length < 40) return false; // IPv4 + TCP minimum
            if ((packetData[0] >> 4) != 4) return false; // IPv4 only
            if (packetData[9] != 6) return false; // TCP protocol
            
            // Parse addresses to check if localhost
            var srcAddr = new IPAddress(packetData.Slice(12, 4));
            var dstAddr = new IPAddress(packetData.Slice(16, 4));
            
            if (!isLocalhost(srcAddr) && !isLocalhost(dstAddr))
                return false;
                
            // Process through TCP stack
            tcpStack.ProcessIncoming(packetData);
            return true;
        }
        catch
        {
            return false;
        }
    }
}