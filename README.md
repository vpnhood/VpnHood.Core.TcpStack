# VpnHood.Core.TcpStack

A lightweight TCP stack library for .NET, designed to intercept and handle TCP connections inside a VPN adapter (TUN/WinDivert). Optimized for virtual network scenarios where congestion control is unnecessary and packet loss is not expected.

## Features

- **Library**: Pure class library — no executable entry point
- **Channel-based**: Uses .NET Channels for efficient async data flow
- **Stream Interface**: Provides standard .NET `Stream` API via `LocalTcpStream`
- **Multiple Connections**: Supports concurrent TCP connections
- **VpnHood Integration**: Works directly with `VpnHood.Core.Packets` and VPN adapters (e.g. WinDivert)
- **No Congestion Control**: Optimized for reliable virtual/loopback connections

## Key Components

### `LocalTcpStack`
The main TCP stack. Processes raw incoming IP packets and manages connections.

```csharp
var tcpStack = new LocalTcpStack();

// Forward outgoing TCP packets back through the adapter
tcpStack.OnPacketSend = packet =>
{
    adapter.SendPacketQueued(packet);
};

// Feed incoming packets from the adapter into the stack
adapter.PacketReceived += (_, packet) =>
{
    tcpStack.ProcessIncoming(packet.Buffer.Span);
};
```

### `LocalTcpListener`
Listens for incoming TCP connections on a virtual endpoint, similar to `TcpListener`.

```csharp
var listener = tcpStack.Listen(new IpEndPointValue(IPAddress.Parse("11.0.0.1"), 8080));

await foreach (var stream in listener.AcceptAllAsync())
{
    _ = Task.Run(() => HandleConnection(stream));
}

listener.Stop();
```

### `LocalTcpStream`
A standard .NET `Stream` implementation for each accepted TCP connection.

```csharp
var buffer = new byte[4096];
int bytesRead = await stream.ReadAsync(buffer);

await stream.WriteAsync(responseData);

await stream.DisposeAsync();
```

## Integration with a VPN Adapter

```csharp
var tcpStack = new LocalTcpStack();

// Step 1: Forward outgoing packets from the stack through the adapter
tcpStack.OnPacketSend = packet =>
{
    adapter.SendPacketQueued(packet);
};

// Step 2: Feed packets received by the adapter into the stack
adapter.PacketReceived += (_, packet) =>
{
    tcpStack.ProcessIncoming(packet.Buffer.Span);
};

// Step 3: Listen for connections on your virtual IP/port
var listener = tcpStack.Listen(new IpEndPointValue(IPAddress.Parse("11.0.0.1"), 8080));
_ = Task.Run(async () =>
{
    await foreach (var stream in listener.AcceptAllAsync())
    {
        _ = Task.Run(() => HandleConnection(stream));
    }
});

// Step 4: Start the adapter, including the virtual IP in IncludeNetworks
await adapter.Start(new VpnAdapterOptions
{
    VirtualIpNetworkV4 = IpNetwork.Parse("10.0.0.0/24"),
    IncludeNetworks = [new IpNetwork(IPAddress.Parse("11.0.0.1"), 32)]
}, CancellationToken.None);
```

## Example: Echo Server

```csharp
var tcpStack = new LocalTcpStack();

tcpStack.OnPacketSend = packet => adapter.SendPacketQueued(packet);
adapter.PacketReceived += (_, packet) => tcpStack.ProcessIncoming(packet.Buffer.Span);

var listener = tcpStack.Listen(new IpEndPointValue(IPAddress.Parse("11.0.0.1"), 8080));

await foreach (var stream in listener.AcceptAllAsync())
{
    _ = Task.Run(async () =>
    {
        try
        {
            var buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = await stream.ReadAsync(buffer)) > 0)
                await stream.WriteAsync(buffer.AsMemory(0, bytesRead));
        }
        finally
        {
            await stream.DisposeAsync();
        }
    });
}
```

## Architecture

```
Adapter (WinDivert/TUN)
    |  PacketReceived
    v
LocalTcpStack.ProcessIncoming()
    |  SYN  --> LocalTcpListener (AcceptAllAsync)
    |  Data --> LocalTcpConnection --> LocalTcpStream (Read)
    |
    |  LocalTcpStream (Write)
    v
LocalTcpStack.OnPacketSend
    |
    v
Adapter.SendPacketQueued()
```

## Limitations

- **No Congestion Control**: Designed for virtual/loopback where packet loss does not occur
- **IPv4 Only**: Currently supports IPv4
- **Simplified TCP State Machine**: Covers the states needed for reliable local connections

## Dependencies

- .NET 10.0+
- `VpnHood.Core.Packets`
