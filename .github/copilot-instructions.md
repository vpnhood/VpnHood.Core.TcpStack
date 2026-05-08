- Do not implement not supported method with GetAwaiter().GetResult()
- We do not need to support congection control because it is loopback and we expect there is no packet loss
- Use primary Constructor when possible
- Our term: Upload mean get packets from a tun adapter and send them to TcpStack to reassemble to stream. Download mean sending data to TcpStack stream and generate tcp packets to send to tun adapter. So Upload is from tun to TcpStack, Download is from TcpStack to tun.
- tests max time out should be 20 seconds, otherwise it is likely a deadlock and we should fail fast instead of waiting for a long time.
- We can use VhLogger.Instance.Trace in hostpath without removing it later , but we need to set VhLoggger.MinLogLevel to Trace

# To diagnose lower level libraris remove their nugets and add them from here. We will remove them later.
AndroidTun: "(SoutionDir)..\VpnHood\VpnHood.Core.VpnAdapters.AndroidTun"
WinDivert: "(SoutionDir)..\VpnHood\VpnHood.Core.VpnAdapters.WinDivert"

