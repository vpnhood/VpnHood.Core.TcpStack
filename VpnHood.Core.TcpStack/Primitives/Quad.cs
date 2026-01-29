using System.Net;
using VpnHood.Core.Toolkit.Logging;

namespace VpnHood.Core.TcpStack.Primitives;

internal readonly record struct Quad(IPEndPoint Source, IPEndPoint Destination)
{
    public override string ToString() => $"{VhLogger.Format(Source)}->{VhLogger.Format(Destination)}";
}
