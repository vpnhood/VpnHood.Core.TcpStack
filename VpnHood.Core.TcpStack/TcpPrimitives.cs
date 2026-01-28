using System.Security.Cryptography;

namespace VpnHood.Core.TcpStack;

internal static class TcpUtil
{
    public static uint NewIsn() => (uint)RandomNumberGenerator.GetInt32(int.MaxValue);
}
