using System.Net;

namespace VpnHood.Core.TcpStack.Primitives;

internal readonly record struct Quad(IPEndPoint Source, IPEndPoint Destination)
{
    public override string ToString() => $"{Source}->{Destination}";
}
