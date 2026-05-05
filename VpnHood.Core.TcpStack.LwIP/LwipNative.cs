using System.Runtime.InteropServices;

namespace VpnHood.Core.TcpStack.LwIP;

/// <summary>
/// P/Invoke declarations for the native lwip_shim library.
/// </summary>
internal static partial class LwipNative
{
    private const string LibName = "liblwip_shim";

    // --- Callback delegates (called from native code) ---
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public unsafe delegate void OutputCallback(byte* data, int len, nint userData);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public unsafe delegate nint AcceptCallback(
        nint conn,
        uint localIp4, ushort localPort,
        uint remoteIp4, ushort remotePort,
        byte* localIp6, byte* remoteIp6,
        int isIpv6,
        nint userData);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public unsafe delegate int RecvCallback(nint conn, byte* data, int len, nint connCtx);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void ClosedCallback(nint conn, int err, nint connCtx);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void SentCallback(nint conn, int len, nint connCtx);

    // --- Stack lifecycle ---
    [LibraryImport(LibName, EntryPoint = "lwip_shim_create")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial nint Create();

    [LibraryImport(LibName, EntryPoint = "lwip_shim_destroy")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial void Destroy(nint stack);

    // --- Callbacks ---
    [LibraryImport(LibName, EntryPoint = "lwip_shim_set_output_callback")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial void SetOutputCallback(nint stack, OutputCallback fn, nint userData);

    [LibraryImport(LibName, EntryPoint = "lwip_shim_set_accept_callback")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial void SetAcceptCallback(nint stack, AcceptCallback fn, nint userData);

    [LibraryImport(LibName, EntryPoint = "lwip_shim_set_recv_callback")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial void SetRecvCallback(nint stack, RecvCallback fn);

    [LibraryImport(LibName, EntryPoint = "lwip_shim_set_closed_callback")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial void SetClosedCallback(nint stack, ClosedCallback fn);

    [LibraryImport(LibName, EntryPoint = "lwip_shim_set_sent_callback")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial void SetSentCallback(nint stack, SentCallback fn);

    // --- Packet I/O ---
    [LibraryImport(LibName, EntryPoint = "lwip_shim_input")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static unsafe partial int Input(nint stack, byte* data, int len);

    // --- Listening ---
    [LibraryImport(LibName, EntryPoint = "lwip_shim_listen_any")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial nint ListenAny(nint stack);

    [LibraryImport(LibName, EntryPoint = "lwip_shim_stop_listen")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial void StopListen(nint stack, nint listener);

    // --- Connection operations ---
    [LibraryImport(LibName, EntryPoint = "lwip_shim_write")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static unsafe partial int Write(nint stack, nint conn, byte* data, int len);

    [LibraryImport(LibName, EntryPoint = "lwip_shim_sndbuf")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial int SndBuf(nint stack, nint conn);

    [LibraryImport(LibName, EntryPoint = "lwip_shim_close")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial void Close(nint stack, nint conn);

    [LibraryImport(LibName, EntryPoint = "lwip_shim_abort")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial void Abort(nint stack, nint conn);

    // --- Timer/poll ---
    [LibraryImport(LibName, EntryPoint = "lwip_shim_poll")]
    [UnmanagedCallConv(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    public static partial void Poll(nint stack);
}
