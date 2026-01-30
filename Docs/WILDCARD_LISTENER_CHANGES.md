# Wildcard Listener Changes

## Summary
Made `LocalTcpListener.LocalEndPoint` nullable to properly support wildcard listeners that accept both IPv4 and IPv6 connections without relying on address comparisons.

## Key Changes

### 1. LocalTcpListener.LocalEndPoint is now nullable
- **Before**: `public IpEndPointValue LocalEndPoint { get; }`
- **After**: `public IpEndPointValue? LocalEndPoint { get; }`
- When `null`, the listener accepts connections on ANY endpoint (both IPv4 and IPv6)

### 2. Added IsAny property
```csharp
public bool IsAny => !LocalEndPoint.HasValue;
```
Provides a clear way to check if a listener is a wildcard listener.

### 3. ListenAny() now passes null
- **Before**: Created `IpEndPointValue(IPAddress.IPv6Any, 0)` 
- **After**: Passes `null` to indicate wildcard
- This prevents incorrect address family comparisons

### 4. Stop handling updated
- Checks `LocalEndPoint.HasValue` before calling `StopListening`
- Calls new `StopListeningAny()` method for wildcard listeners

### 5. StopListening logic simplified
- Removed comparison check: `_anyListener.LocalEndPoint.Equals(localEndPoint)`
- Added dedicated `StopListeningAny()` internal method

## Why This Is Important

### Problem with Previous Approach
When `ListenAny()` created a listener with `IPAddress.IPv6Any`, it could cause issues:
1. **Address Family Mismatch**: IPv6Any wouldn't match IPv4 packets
2. **Comparison Issues**: Can't properly compare a wildcard address with specific endpoints
3. **Confusion**: IPv6Any suggests IPv6-only, but we want both IPv4 and IPv6

### Solution Benefits
1. **Clear Intent**: `null` clearly indicates "any endpoint"
2. **No Comparison Issues**: Wildcard listeners are identified by null check, not address comparison
3. **Correct Packet Construction**: Code already uses actual packet endpoints (`ipEndPointQuad.Destination/Source`), not `listener.LocalEndPoint`
4. **Type Safety**: Nullable type forces proper handling at compile time

## Packet Flow Verification

### SYN Packet Handling (HandleSynPacket)
✅ Uses `ipEndPointQuad.Destination` and `ipEndPointQuad.Source` from the incoming packet
✅ Never uses `listener.LocalEndPoint` for packet construction
✅ Works correctly for both specific and wildcard listeners

### SYN-ACK Response
```csharp
var synAckPacket = PacketBuilder.BuildTcp(
    ipEndPointQuad.Destination,  // ✅ Actual destination from packet
    ipEndPointQuad.Source,       // ✅ Actual source from packet
    ...);
```

### Connection Creation
```csharp
var tcpConnection = new LocalTcpConnection(
    ipEndPointQuad,  // ✅ Contains actual endpoints from packet
    ...);
```

## Test Scenarios

### IPv4 Connection to Wildcard Listener
1. Client connects to `10.0.0.1:8080` (IPv4)
2. Wildcard listener has `LocalEndPoint = null`
3. Packet arrives with `Destination=10.0.0.1:8080, Source=10.0.0.2:12345`
4. SYN-ACK sent from `10.0.0.1:8080` to `10.0.0.2:12345` ✅

### IPv6 Connection to Wildcard Listener
1. Client connects to `[::1]:8080` (IPv6)
2. Wildcard listener has `LocalEndPoint = null`
3. Packet arrives with `Destination=[::1]:8080, Source=[::1]:54321`
4. SYN-ACK sent from `[::1]:8080` to `[::1]:54321` ✅

### Specific Listener (No Changes)
1. `Listen(new IpEndPointValue(IPAddress.Parse("127.0.0.1"), 8080))`
2. `LocalEndPoint = 127.0.0.1:8080` (not null)
3. Only matches packets to exactly `127.0.0.1:8080` ✅

## Migration Guide

If you have existing code:
```csharp
// Before
var listener = tcpStack.ListenAny();
var endpoint = listener.LocalEndPoint; // Was IPv6Any:0
```

Now:
```csharp
// After
var listener = tcpStack.ListenAny();
var endpoint = listener.LocalEndPoint; // Now null
var isWildcard = listener.IsAny;  // true
```

**Important**: The actual endpoint used for packet construction comes from the incoming packet, not from `LocalEndPoint`, so this change doesn't affect functionality—it just makes the API clearer and safer.
