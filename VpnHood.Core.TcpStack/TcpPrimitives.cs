using System.Net;
using System.Security.Cryptography;

namespace VpnHood.Core.TcpStack;

[Flags]
internal enum TcpFlags : byte
{
    None = 0,
    Fin = 0x01,
    Syn = 0x02,
    Rst = 0x04,
    Psh = 0x08,
    Ack = 0x10,
}

internal readonly struct Quad : IEquatable<Quad>
{
    public readonly IPEndPoint Source;
    public readonly IPEndPoint Destination;

    public Quad(IPEndPoint source, IPEndPoint destination)
    {
        Source = source;
        Destination = destination;
    }

    public bool Equals(Quad other) => Source.Equals(other.Source) && Destination.Equals(other.Destination);
    public override bool Equals(object? obj) => obj is Quad q && Equals(q);
    public override int GetHashCode() => HashCode.Combine(Source, Destination);
    public override string ToString() => $"{Source}->{Destination}";
}

internal static class TcpUtil
{
    public static uint NewIsn() => (uint)RandomNumberGenerator.GetInt32(int.MaxValue);
}
