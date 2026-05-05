@echo off
call "C:\Program Files\Microsoft Visual Studio\18\Community\Common7\Tools\VsDevCmd.bat" -arch=amd64 >nul 2>&1

cd /d "c:\Users\Developer\source\repos\Vh\VpnHood.Core.TcpStack\VpnHood.Core.TcpStack.LwIP\native"

cl.exe /nologo /O2 /LD /DNDEBUG /D_CRT_SECURE_NO_WARNINGS ^
  /I"c:\Users\Developer\source\repos\Vh\VpnHood.Core.TcpStack\VpnHood.Core.TcpStack.LwIP\native" ^
  /I"C:\Users\Developer\source\repos\_Test\lwip\src\include" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\init.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\def.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\inet_chksum.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\ip.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\mem.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\memp.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\netif.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\pbuf.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\tcp.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\tcp_in.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\tcp_out.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\timeouts.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\ipv4\icmp.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\ipv4\ip4.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\ipv4\ip4_addr.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\ipv4\ip4_frag.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\ipv6\icmp6.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\ipv6\ip6.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\ipv6\ip6_addr.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\ipv6\ip6_frag.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\ipv6\nd6.c" ^
  "C:\Users\Developer\source\repos\_Test\lwip\src\core\ipv6\inet6.c" ^
  "c:\Users\Developer\source\repos\Vh\VpnHood.Core.TcpStack\VpnHood.Core.TcpStack.LwIP\native\lwip_shim.c" ^
  /Fe:"c:\Users\Developer\source\repos\Vh\VpnHood.Core.TcpStack\VpnHood.Core.TcpStack.LwIP\runtimes\win-x64\native\lwip_shim.dll" /link /DLL
