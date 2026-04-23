using System.Net;
using VpnHood.Core.Packets;
using VpnHood.Core.Toolkit.Net;

namespace VpnHood.Core.TcpStack;

/// <summary>
/// Integration helpers for connecting <see cref="LocalTcpStack"/> with a TUN VPN adapter.
/// </summary>
public static class TcpStackIntegration
{
    /// <summary>
    /// Wires the stack's <see cref="LocalTcpStack.OnPacketSend"/> to the supplied callback.
    /// The callback receives the raw packet bytes; the stack disposes the underlying
    /// pooled <see cref="IpPacket"/> after the callback returns.
    /// </summary>
    public static void IntegrateWithAdapter(
        this LocalTcpStack tcpStack,
        Action<byte[]> writePacketCallback,
        Func<IPAddress, bool>? isLocalhost = null)
    {
        _ = isLocalhost; // accepted for API symmetry; not used here
        tcpStack.OnPacketSend = packet => {
            try {
                // Copy out the bytes; the stack/adapter manages packet lifetime.
                writePacketCallback(packet.Buffer.ToArray());
            }
            finally {
                packet.Dispose();
            }
        };
    }

    /// <summary>
    /// Quickly checks if the packet is a TCP packet to/from a localhost address; if so,
    /// dispatches it through the TCP stack and returns true. Supports both IPv4 and IPv6.
    /// </summary>
    public static bool TryProcessPacket(
        this LocalTcpStack tcpStack,
        ReadOnlySpan<byte> packetData,
        Func<IPAddress, bool>? isLocalhost = null)
    {
        if (packetData.Length < 20) return false; // minimum IPv4 header

        try {
            isLocalhost ??= IPAddress.IsLoopback;
            var version = packetData[0] >> 4;

            switch (version) {
                case 4: {
                    if (packetData.Length < 40) return false;       // v4 + min TCP
                    if (packetData[9] != (byte)IpProtocol.Tcp) return false;
                    var src = new IPAddress(packetData.Slice(12, 4));
                    var dst = new IPAddress(packetData.Slice(16, 4));
                    if (!isLocalhost(src) && !isLocalhost(dst)) return false;
                    tcpStack.ProcessIncoming(packetData);
                    return true;
                }
                case 6: {
                    if (packetData.Length < 60) return false;       // v6 + min TCP
                    if (packetData[6] != (byte)IpProtocol.Tcp) return false; // no extension headers
                    var src = new IPAddress(packetData.Slice(8, 16));
                    var dst = new IPAddress(packetData.Slice(24, 16));
                    if (!isLocalhost(src) && !isLocalhost(dst)) return false;
                    tcpStack.ProcessIncoming(packetData);
                    return true;
                }
                default:
                    return false;
            }
        }
        catch {
            return false;
        }
    }
}
