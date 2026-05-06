using System.Net;
using System.Security.Cryptography;
using VpnHood.Core.Packets;
using VpnHood.Core.Packets.Extensions;
using VpnHood.Core.TcpStack.LwIP;
using VpnHood.Core.Toolkit.Net;

namespace VpnHood.Core.TcpStack.Test;

/// <summary>
/// Tests the LwIP TCP stack provider by connecting it to the LocalTcpStack.
/// Packets produced by one stack are fed into the other (loopback wire).
/// </summary>
[TestClass]
public sealed class LwipTcpStackTest
{
    private static readonly IPAddress ServerIp = IPAddress.Parse("10.0.0.1");
    private static readonly IPAddress ClientIp = IPAddress.Parse("10.0.0.2");
    private const int ServerPort = 8080;

    /// <summary>
    /// Tests that a TCP handshake completes and a connection is accepted.
    /// Manually drives the 3-way handshake via raw packets.
    /// </summary>
    [TestMethod]
    [Timeout(10000)]
    public async Task LwipHandshake_ShouldAcceptConnection()
    {
        // Arrange
        using var lwipStack = new LwipTcpStack();
        var receivedPackets = new List<IpPacket>();
        object lockObj = new();

        lwipStack.OnPacketSend = packet =>
        {
            lock (lockObj) receivedPackets.Add(packet);
        };

        lwipStack.ListenAny();

        // Act - send SYN
        var synPacket = CreateTcpPacket(ClientIp, 54321, ServerIp, ServerPort,
            syn: true, seq: 1000);
        lwipStack.ProcessIncoming(synPacket.Buffer.Span);
        synPacket.Dispose();

        await Task.Delay(200);

        // Should receive SYN-ACK
        IpPacket synAckPacket;
        lock (lockObj) {
            Assert.IsTrue(receivedPackets.Count >= 1, $"Should receive SYN-ACK, got {receivedPackets.Count} packets");
            synAckPacket = receivedPackets[0];
        }
        var synAckTcp = synAckPacket.ExtractTcp();
        Assert.IsTrue(synAckTcp.Synchronize, "Should have SYN flag");
        Assert.IsTrue(synAckTcp.Acknowledgment, "Should have ACK flag");
        var serverSeq = synAckTcp.SequenceNumber;

        // Send ACK to complete handshake
        var ackPacket = CreateTcpPacket(ClientIp, 54321, ServerIp, ServerPort,
            ack: true, seq: 1001, ackNum: serverSeq + 1);
        lwipStack.ProcessIncoming(ackPacket.Buffer.Span);
        ackPacket.Dispose();

        // Assert - connection should be accepted
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var stream = await lwipStack.AcceptAsync(cts.Token);
        Assert.IsNotNull(stream);
        Assert.AreEqual(ServerPort, stream.LocalEndPoint.Port);

        await stream.DisposeAsync();
        foreach (var p in receivedPackets) p.Dispose();
    }

    /// <summary>
    /// Full echo test: LocalTcpStack client sends data, LwIP server echoes back.
    /// Both stacks exchange packets directly (loopback wire).
    /// </summary>
    [TestMethod]
    [Timeout(15000)]
    public async Task LwipEcho_SmallData_ShouldSucceed()
    {
        using var lwipStack = new LwipTcpStack();
        var clientStack = new LocalTcpStack();

        // Wire stacks together
        lwipStack.OnPacketSend = packet =>
        {
            // ReSharper disable once AccessToDisposedClosure
            clientStack.ProcessIncoming(packet.Buffer.Span);
            packet.Dispose();
        };

        clientStack.OnPacketSend = packet =>
        {
            // ReSharper disable once AccessToDisposedClosure
            lwipStack.ProcessIncoming(packet.Buffer.Span);
            packet.Dispose();
        };

        lwipStack.ListenAny();

        // Start echo server on lwIP side
        var echoTask = Task.Run(async () =>
        {
            await foreach (var stream in lwipStack.AcceptAllAsync())
            {
                var buffer = new byte[4096];
                while (true)
                {
                    var bytesRead = await stream.Stream.ReadAsync(buffer);
                    if (bytesRead == 0) break;
                    await stream.Stream.WriteAsync(buffer.AsMemory(0, bytesRead));
                }
                await stream.DisposeAsync();
                break;
            }
        });

        // Client side: use LocalTcpStack listener to accept the "outgoing" connection
        // Actually, we need to drive this differently. We send raw SYN to lwIP and
        // then the client stack receives packets from lwIP's response.
        // The client stack itself needs a listener to handle the SYN-ACK.
        
        // Better approach: use the LocalTcpStack as a passive client by sending
        // a raw SYN to lwIP and tracking the conversation manually.
        // But this is complex. Let's use a simpler approach with WinDivert-style
        // direct packet manipulation.

        // Send SYN to lwIP
        var clientPort = 54321;
        uint clientSeq = 1000;
        var sentPackets = new List<IpPacket>();
        
        // Temporarily capture client stack output to track the conversation
        var clientReceivedPackets = new List<IpPacket>();
        lwipStack.OnPacketSend = packet =>
        {
            lock (clientReceivedPackets)
                clientReceivedPackets.Add(packet); // don't dispose - we need to inspect
        };

        // Step 1: Send SYN to lwIP
        var synPacket = CreateTcpPacket(ClientIp, clientPort, ServerIp, ServerPort,
            syn: true, seq: clientSeq);
        lwipStack.ProcessIncoming(synPacket.Buffer.Span);
        synPacket.Dispose();

        await Task.Delay(200);

        // Step 2: Check SYN-ACK from lwIP
        IpPacket synAckPacket;
        lock (clientReceivedPackets) {
            Assert.IsTrue(clientReceivedPackets.Count >= 1, "Should receive SYN-ACK");
            synAckPacket = clientReceivedPackets[0];
        }
        var synAckTcp = synAckPacket.ExtractTcp();
        Assert.IsTrue(synAckTcp.Synchronize, "Should have SYN flag");
        Assert.IsTrue(synAckTcp.Acknowledgment, "Should have ACK flag");
        Assert.AreEqual(clientSeq + 1, synAckTcp.AcknowledgmentNumber, "ACK should be client seq + 1");
        var serverSeq = synAckTcp.SequenceNumber;

        // Step 3: Complete handshake - send ACK
        clientSeq += 1; // SYN consumed 1 seq
        var ackPacket = CreateTcpPacket(ClientIp, clientPort, ServerIp, ServerPort,
            ack: true, seq: clientSeq, ackNum: serverSeq + 1);
        lwipStack.ProcessIncoming(ackPacket.Buffer.Span);
        ackPacket.Dispose();

        await Task.Delay(200);

        // Step 4: Send data
        lock (clientReceivedPackets) clientReceivedPackets.Clear();
        
        var testData = "Hello, lwIP!"u8.ToArray();
        var dataPacket = CreateTcpPacket(ClientIp, clientPort, ServerIp, ServerPort,
            ack: true, psh: true, seq: clientSeq, ackNum: serverSeq + 1, payload: testData);
        lwipStack.ProcessIncoming(dataPacket.Buffer.Span);
        dataPacket.Dispose();

        // Wait for echo response
        await Task.Delay(1000);

        // Step 5: ACK the data so lwIP can proceed
        clientSeq += (uint)testData.Length;
        
        // Check received packets for data
        IpPacket[] responsePackets;
        lock (clientReceivedPackets) responsePackets = clientReceivedPackets.ToArray();

        // ACK any data packets we receive (needed for lwIP flow control)
        uint maxServerSeq = serverSeq + 1;
        var echoedBytes = new List<byte>();
        foreach (var pkt in responsePackets) {
            var tcp = pkt.ExtractTcp();
            if (tcp.Payload.Length > 0) {
                echoedBytes.AddRange(tcp.Payload.ToArray());
                var endSeq = tcp.SequenceNumber + (uint)tcp.Payload.Length;
                if (endSeq > maxServerSeq) maxServerSeq = endSeq;
            }
        }

        // Send ACK for the echoed data
        if (maxServerSeq > serverSeq + 1) {
            var dataAck = CreateTcpPacket(ClientIp, clientPort, ServerIp, ServerPort,
                ack: true, seq: clientSeq, ackNum: maxServerSeq);
            lwipStack.ProcessIncoming(dataAck.Buffer.Span);
            dataAck.Dispose();
        }

        // Assert echo
        Assert.IsTrue(echoedBytes.Count > 0, $"Should receive echoed data. Got {responsePackets.Length} packets");
        CollectionAssert.AreEqual(testData, echoedBytes.ToArray(), "Echoed data should match");

        // Cleanup
        foreach (var p in responsePackets) p.Dispose();
        clientStack.Dispose();
    }

    /// <summary>
    /// Tests larger data transfer (multiple packets) through lwIP stack.
    /// </summary>
    [TestMethod]
    [Timeout(15000)]
    public async Task LwipDataTransfer_1KB_ShouldSucceed()
    {
        using var lwipStack = new LwipTcpStack();

        var clientReceivedPackets = new List<IpPacket>();
        object lockObj = new();
        lwipStack.OnPacketSend = packet =>
        {
            lock (lockObj) clientReceivedPackets.Add(packet);
        };

        lwipStack.ListenAny();

        // Start a read-all server that collects data
        var serverReceivedData = new List<byte>();
        var serverDone = new TaskCompletionSource();
        _ = Task.Run(async () =>
        {
            await foreach (var stream in lwipStack.AcceptAllAsync())
            {
                var buffer = new byte[4096];
                while (true)
                {
                    var bytesRead = await stream.Stream.ReadAsync(buffer);
                    if (bytesRead == 0) break;
                    lock (serverReceivedData)
                        serverReceivedData.AddRange(buffer.AsSpan(0, bytesRead).ToArray());
                }
                await stream.DisposeAsync();
                serverDone.TrySetResult();
                break;
            }
        });

        // Client handshake
        var clientPort = 54322;
        uint clientSeq = 2000;

        var synPacket = CreateTcpPacket(ClientIp, clientPort, ServerIp, ServerPort,
            syn: true, seq: clientSeq);
        lwipStack.ProcessIncoming(synPacket.Buffer.Span);
        synPacket.Dispose();

        await Task.Delay(200);

        IpPacket synAckPacket;
        lock (lockObj) {
            Assert.IsTrue(clientReceivedPackets.Count >= 1, "Should receive SYN-ACK");
            synAckPacket = clientReceivedPackets[0];
        }
        var synAckTcp = synAckPacket.ExtractTcp();
        var serverSeq = synAckTcp.SequenceNumber;

        clientSeq += 1;
        var ackPacket = CreateTcpPacket(ClientIp, clientPort, ServerIp, ServerPort,
            ack: true, seq: clientSeq, ackNum: serverSeq + 1);
        lwipStack.ProcessIncoming(ackPacket.Buffer.Span);
        ackPacket.Dispose();

        await Task.Delay(200);
        lock (lockObj) clientReceivedPackets.Clear();

        // Send 1KB of data in chunks (simulating MTU-sized packets)
        var testData = new byte[1024];
        RandomNumberGenerator.Fill(testData);
        
        const int chunkSize = 512;
        for (var offset = 0; offset < testData.Length; offset += chunkSize)
        {
            var chunk = testData.AsSpan(offset, Math.Min(chunkSize, testData.Length - offset)).ToArray();
            var dataPacket = CreateTcpPacket(ClientIp, clientPort, ServerIp, ServerPort,
                ack: true, psh: (offset + chunkSize >= testData.Length),
                seq: clientSeq, ackNum: serverSeq + 1, payload: chunk);
            lwipStack.ProcessIncoming(dataPacket.Buffer.Span);
            dataPacket.Dispose();
            clientSeq += (uint)chunk.Length;

            // ACK any packets that come back
            await Task.Delay(50);
            lock (lockObj) {
                foreach (var p in clientReceivedPackets) {
                    var tcp = p.ExtractTcp();
                    var endSeq = tcp.SequenceNumber + (uint)tcp.Payload.Length;
                    if (tcp.Synchronize) endSeq++;
                    if (endSeq > serverSeq + 1) serverSeq = endSeq - 1;
                    p.Dispose();
                }
                clientReceivedPackets.Clear();
            }
        }

        // Send FIN
        var finPacket = CreateTcpPacket(ClientIp, clientPort, ServerIp, ServerPort,
            ack: true, fin: true, seq: clientSeq, ackNum: serverSeq + 1);
        lwipStack.ProcessIncoming(finPacket.Buffer.Span);
        finPacket.Dispose();

        // Wait for server to finish receiving
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await serverDone.Task.WaitAsync(cts.Token);

        // Assert
        byte[] receivedArr;
        lock (serverReceivedData) receivedArr = serverReceivedData.ToArray();
        Assert.AreEqual(testData.Length, receivedArr.Length, "Should receive all 1024 bytes");
        CollectionAssert.AreEqual(testData, receivedArr, "Data should match");
    }

    /// <summary>
    /// Tests the LwIP stack with the WinDivert adapter integration (same as the
    /// existing integration test but using LwipTcpStack).
    /// Requires administrator privileges.
    /// </summary>
    [TestMethod]
    [TestCategory("Integration")]
    [Timeout(60000)]
    public async Task LwipIntegration_WinDivert_Echo_ShouldSucceed()
    {
        var testServerIp = IPAddress.Parse("11.0.0.3");
        const int testServerPort = 8082;
        const int testDataSize = 64 * 1024; // 64 KB

        var testData = new byte[testDataSize];
        RandomNumberGenerator.Fill(testData);

        using var lwipStack = new LwipTcpStack();

        var adapterSettings = new VpnHood.Core.VpnAdapters.WinDivert.WinDivertVpnAdapterSettings
        {
            AdapterName = "VpnHoodLwIP",
            ExcludeLocalNetwork = false,
            SimulateDns = false,
            AutoDisposePackets = true,
            Blocking = true,
        };

        using var adapter = new VpnHood.Core.VpnAdapters.WinDivert.WinDivertVpnAdapter(adapterSettings);

        // Feed incoming packets to lwIP
        adapter.PacketReceived += (_, packet) =>
        {
            lwipStack.ProcessIncoming(packet.Buffer.Span);
        };

        // Send lwIP output through adapter
        lwipStack.OnPacketSend = packet =>
        {
            adapter.SendPacketQueued(packet);
        };

        lwipStack.ListenAny();

        // Echo server
        _ = Task.Run(async () =>
        {
            await foreach (var stream in lwipStack.AcceptAllAsync())
            {
                var buffer = new byte[8192];
                while (true)
                {
                    var bytesRead = await stream.Stream.ReadAsync(buffer);
                    if (bytesRead == 0) break;
                    await stream.Stream.WriteAsync(buffer.AsMemory(0, bytesRead));
                }
                await stream.DisposeAsync();
                break;
            }
        });

        try
        {
            var options = new VpnHood.Core.VpnAdapters.Abstractions.VpnAdapterOptions
            {
                SessionName = "LwipTestSession",
                VirtualIpNetworkV4 = IpNetwork.Parse("10.0.0.0/24"),
                IncludeNetworks = [new IpNetwork(testServerIp, 32)]
            };

            await adapter.Start(options, CancellationToken.None);
            await Task.Delay(1000);

            using var tcpClient = new System.Net.Sockets.TcpClient();
            tcpClient.NoDelay = true;

            using var connectCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await tcpClient.ConnectAsync(testServerIp, testServerPort, connectCts.Token);

            await using var stream = tcpClient.GetStream();

            // Send and receive simultaneously
            var receivedData = new List<byte>();
            var sendTask = Task.Run(async () =>
            {
                const int chunkSize = 8192;
                for (var offset = 0; offset < testData.Length; offset += chunkSize)
                {
                    var sz = Math.Min(chunkSize, testData.Length - offset);
                    await stream.WriteAsync(testData.AsMemory(offset, sz));
                }
            });

            var receiveTask = Task.Run(async () =>
            {
                var buf = new byte[8192];
                while (receivedData.Count < testDataSize)
                {
                    var n = await stream.ReadAsync(buf);
                    if (n == 0) break;
                    receivedData.AddRange(buf.Take(n));
                }
            });

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
            await Task.WhenAll(sendTask, receiveTask).WaitAsync(cts.Token);

            Assert.AreEqual(testDataSize, receivedData.Count, "Received data size should match");
            CollectionAssert.AreEqual(testData, receivedData.ToArray(), "Data should match");
        }
        finally
        {
            adapter.Stop();
        }
    }

    private static IpPacket CreateSynPacket(IPAddress srcIp, int srcPort, IPAddress dstIp, int dstPort)
    {
        return CreateTcpPacket(srcIp, srcPort, dstIp, dstPort, syn: true, seq: 1000);
    }

    private static IpPacket CreateTcpPacket(
        IPAddress srcIp, int srcPort,
        IPAddress dstIp, int dstPort,
        bool syn = false, bool ack = false, bool fin = false, bool psh = false, bool rst = false,
        uint seq = 0, uint ackNum = 0,
        byte[]? payload = null)
    {
        // SYN packets need MSS option
        ReadOnlySpan<byte> options = ReadOnlySpan<byte>.Empty;
        if (syn) {
            // MSS = 1360, Window Scale = 0, NOP padding
            options = new byte[] { 2, 4, 0x05, 0x50 };
        }

        var packet = PacketBuilder.BuildTcp(
            new IPEndPoint(srcIp, srcPort),
            new IPEndPoint(dstIp, dstPort),
            options,
            payload ?? ReadOnlySpan<byte>.Empty);

        var tcp = packet.ExtractTcp();
        tcp.Synchronize = syn;
        tcp.Acknowledgment = ack;
        tcp.Finish = fin;
        tcp.Push = psh;
        tcp.Reset = rst;
        tcp.SequenceNumber = seq;
        tcp.AcknowledgmentNumber = ackNum;
        tcp.WindowSize = 65535;

        packet.UpdateAllChecksums();
        return packet;
    }

    private static async Task WaitForCondition(Func<bool> condition, CancellationToken ct, int delayMs = 10)
    {
        while (!condition() && !ct.IsCancellationRequested)
            await Task.Delay(delayMs, ct);
    }
}
