using System.Net;

namespace VpnHood.Core.TcpStack;

internal readonly record struct Quad(IPEndPoint Source, IPEndPoint Destination)
{
    public override string ToString() => $"{Source}->{Destination}";
}
