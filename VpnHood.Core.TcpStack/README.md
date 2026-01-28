# LocalTcpStack - Lightweight TCP Stack for Localhost

A lightweight, localhost-only TCP stack implementation in C# designed for integration with VpnHood's TunVpnAdapter. This stack is optimized for local connections where packet loss is not expected and congestion control is not needed.

## Features

- **Lightweight**: Simplified TCP implementation for localhost scenarios
- **Channel-based**: Uses .NET Channels for efficient async data flow
- **Stream Interface**: Provides standard .NET Stream API through `LocalTcpStream`
- **Simultaneous Connections**: Supports multiple concurrent TCP connections
- **VpnHood Integration**: Built to work with existing VpnHood.Core.Packets
- **No Congestion Control**: Optimized for reliable localhost connections

## Key Components

### LocalTcpStack
The main TCP stack that processes incoming packets and manages connections.

```csharp
var tcpStack = new LocalTcpStack();

// Set up packet output callback
tcpStack.OnPacketSend = packet => 
{
    // Send packet back to adapter
    adapter.WritePacket(packet);
};

// Process incoming packets
tcpStack.ProcessIncoming(packetData);
```

### LocalTcpListener
TCP listener that accepts incoming connections, similar to `TcpListener`.

```csharp
var listener = tcpStack.Listen(new IPEndPoint(IPAddress.Loopback, 8080));

// Accept a single connection
var stream = await listener.AcceptAsync();

// Or accept all connections using async enumerable
await foreach (var stream in listener.AcceptAllAsync())
{
    // Handle new connection
    _ = Task.Run(() => HandleConnection(stream));
}

// Stop listening
listener.Stop();
```

### LocalTcpStream
A standard .NET Stream implementation for TCP connections.

```csharp
// Read data
var buffer = new byte[1024];
int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);

// Write data
await stream.WriteAsync(data, 0, data.Length);

// Close connection
stream.Dispose();
```

## Integration with TunVpnAdapter

### Step 1: Setup Integration
```csharp
var tcpStack = new LocalTcpStack();

// Integrate with adapter
tcpStack.IntegrateWithAdapter(packetBytes => 
{
    var packet = PacketBuilder.Parse(packetBytes);
    adapter.WritePacket(packet);
});
```

### Step 2: Process Incoming Packets
In your TunVpnAdapter's packet processing:

```csharp
protected override bool ReadPacket(byte[] buffer)
{
    // Try to handle with TCP stack first
    if (tcpStack.TryProcessPacket(buffer))
        return true;
        
    // Continue with normal packet processing
    return base.ReadPacket(buffer);
}
```

### Step 3: Start TCP Services
```csharp
// Start HTTP server on localhost:8080
var httpListener = tcpStack.Listen(new IPEndPoint(IPAddress.Loopback, 8080));
_ = Task.Run(async () =>
{
    await foreach (var stream in httpListener.AcceptAllAsync())
    {
        _ = Task.Run(() => HandleHttpRequest(stream));
    }
});

// Start SOCKS proxy on localhost:1080
var socksListener = tcpStack.Listen(new IPEndPoint(IPAddress.Loopback, 1080));
_ = Task.Run(async () =>
{
    await foreach (var stream in socksListener.AcceptAllAsync())
    {
        _ = Task.Run(() => HandleSocksConnection(stream));
    }
});
```

## Example: Echo Server

```csharp
var tcpStack = new LocalTcpStack();
var listener = tcpStack.Listen(new IPEndPoint(IPAddress.Loopback, 8080));

await foreach (var stream in listener.AcceptAllAsync())
{
    _ = Task.Run(async () =>
    {
        var buffer = new byte[1024];
        int bytesRead;
        
        while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
        {
            // Echo back the data
            await stream.WriteAsync(buffer, 0, bytesRead);
        }
        
        stream.Dispose();
    });
}
```

## Architecture

The TCP stack uses the following flow:

1. **Incoming Packets** ? `ProcessIncoming()` ? Parse TCP headers
2. **SYN Packets** ? Create new `LocalTcpConnection` ? Enqueue in `LocalTcpListener`
3. **Data Packets** ? Route to existing connection ? Push to app via Channels
4. **Outgoing Data** ? App writes to `LocalTcpStream` ? Channels ? TCP packets
5. **TCP Packets** ? `OnPacketSend` callback ? Back to adapter

## Limitations

- **Localhost Only**: Designed for 127.0.0.1 traffic only
- **No Congestion Control**: Assumes reliable, low-latency local connections
- **Simplified State Machine**: Basic TCP state handling for local scenarios
- **IPv4 Only**: Currently supports IPv4 addresses only
- **No TCP Options**: Limited support for TCP options

## Dependencies

- .NET 10.0+
- VpnHood.Core.Packets
- System.Threading.Channels
- System.IO.Pipelines

## Thread Safety

- `LocalTcpStack` is thread-safe for concurrent packet processing
- `LocalTcpStream` should be used by a single thread for reading/writing
- `LocalTcpListener` can be safely accessed by multiple threads