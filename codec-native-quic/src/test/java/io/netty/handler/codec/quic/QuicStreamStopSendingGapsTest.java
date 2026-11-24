/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.quic;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.channel.socket.ChannelOutputShutdownException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cases around STOP_SENDING that netty#18354 does not cover yet.
 */
public class QuicStreamStopSendingGapsTest extends AbstractQuicTest {

    private static final long ERROR_CODE = 100;

    /**
     * The STOP_SENDING arrives before the first data of a stream opened by the peer, so the channel is created
     * already stopped. Its handler must still be told.
     */
    @ParameterizedTest
    @MethodSource("newSslTaskExecutors")
    public void testStopSendingBeforeFirstDataIsReported(Executor executor) throws Throwable {
        Channel server = null;
        Channel channel = null;
        try {
            Streams serverStreams = new Streams();
            server = QuicTestUtils.newServer(executor, QuicTestUtils.NOOP_HANDLER, serverStreams);
            channel = QuicTestUtils.newClient(executor);
            QuicChannel quicChannel = connect(channel, server);

            QuicStreamChannel clientStream = quicChannel.createStream(QuicStreamType.BIDIRECTIONAL,
                    new ChannelInboundHandlerAdapter()).get();
            clientStream.shutdownInput((int) ERROR_CODE).sync();
            // The data of a second stream is sent after the STOP_SENDING, so once the server has seen that stream, it
            // has also processed the STOP_SENDING, before any data of the first stream.
            QuicStreamChannel barrier = quicChannel.createStream(QuicStreamType.BIDIRECTIONAL,
                    new ChannelInboundHandlerAdapter()).get();
            serverStreams.open(barrier);
            QuicStreamChannel serverStream = serverStreams.open(clientStream);

            assertThat(serverStream.isOutputShutdown()).isTrue();
            assertStopped(serverStreams.next());

            quicChannel.close().sync();
        } finally {
            QuicTestUtils.closeIfNotNull(channel);
            QuicTestUtils.closeIfNotNull(server);
            shutdown(executor);
        }
    }

    /**
     * Writes after a STOP_SENDING must fail, also empty ones and ones that would be queued.
     */
    @ParameterizedTest
    @MethodSource("newSslTaskExecutors")
    public void testWritesAfterStopSendingFail(Executor executor) throws Throwable {
        Channel server = null;
        Channel channel = null;
        try {
            Streams serverStreams = new Streams();
            server = QuicTestUtils.newServer(executor, QuicTestUtils.NOOP_HANDLER, serverStreams);
            channel = QuicTestUtils.newClient(executor);
            QuicChannel quicChannel = connect(channel, server);

            QuicStreamChannel clientStream = quicChannel.createStream(QuicStreamType.BIDIRECTIONAL,
                    new ChannelInboundHandlerAdapter()).get();
            QuicStreamChannel serverStream = serverStreams.open(clientStream);
            clientStream.shutdownInput((int) ERROR_CODE).sync();
            assertStopped(serverStreams.next());

            ByteBuf empty = Unpooled.directBuffer(0);
            ChannelFuture emptyWrite = serverStream.eventLoop().submit(
                    () -> serverStream.writeAndFlush(empty)).get();
            ByteBuf composite = Unpooled.wrappedBuffer(
                    Unpooled.directBuffer(8).writeZero(8), Unpooled.directBuffer(8).writeZero(8));
            ChannelFuture compositeWrite = serverStream.eventLoop().submit(
                    () -> serverStream.writeAndFlush(composite)).get();

            assertThat(emptyWrite.isDone()).as("empty write done").isTrue();
            assertThat(emptyWrite.cause()).as("empty write").isInstanceOf(ChannelOutputShutdownException.class);
            assertThat(compositeWrite.isDone()).as("composite write done").isTrue();
            assertThat(compositeWrite.cause()).as("composite write")
                    .isInstanceOf(ChannelOutputShutdownException.class);
            assertThat(serverStream.isWritable()).as("writable after STOP_SENDING").isFalse();

            quicChannel.close().sync();
        } finally {
            QuicTestUtils.closeIfNotNull(channel);
            QuicTestUtils.closeIfNotNull(server);
            shutdown(executor);
        }
    }

    /**
     * The peer finished its side and then stopped ours: a later write must fail, not stay queued
     * (netty#15924), and the stream should close as nothing more can happen on it.
     */
    @ParameterizedTest
    @MethodSource("newSslTaskExecutors")
    public void testWriteAfterFinAndStopSendingFails(Executor executor) throws Throwable {
        Channel server = null;
        Channel channel = null;
        try {
            Streams serverStreams = new Streams();
            server = QuicTestUtils.newServer(executor, QuicTestUtils.NOOP_HANDLER, serverStreams);
            channel = QuicTestUtils.newClient(executor);
            QuicChannel quicChannel = connect(channel, server);

            QuicStreamChannel clientStream = quicChannel.createStream(QuicStreamType.BIDIRECTIONAL,
                    new ChannelInboundHandlerAdapter()).get();
            QuicStreamChannel serverStream = serverStreams.open(clientStream);

            clientStream.shutdownOutput().sync();
            assertThat(serverStreams.next()).isSameAs(ChannelInputShutdownReadComplete.INSTANCE);
            clientStream.shutdownInput((int) ERROR_CODE).sync();
            assertStopped(serverStreams.next());

            ByteBuf buffer = Unpooled.directBuffer(8).writeZero(8);
            ChannelFuture write = serverStream.eventLoop().submit(() -> serverStream.writeAndFlush(buffer)).get();
            assertThat(write.isDone()).as("write done").isTrue();
            // Fails either as output shut down, or as closed once the stream closed itself.
            assertThat(write.cause()).isInstanceOfAny(ChannelOutputShutdownException.class,
                    java.nio.channels.ClosedChannelException.class);

            assertThat(serverStream.closeFuture().await(5, TimeUnit.SECONDS))
                    .as("stream closed after FIN and STOP_SENDING").isTrue();

            quicChannel.close().sync();
        } finally {
            QuicTestUtils.closeIfNotNull(channel);
            QuicTestUtils.closeIfNotNull(server);
            shutdown(executor);
        }
    }

    /**
     * The server shuts down its input (sends STOP_SENDING) and ends its output: the stream should close once the
     * peer ends or resets its side, as it does when the input ended with a FIN.
     */
    @ParameterizedTest
    @MethodSource("newSslTaskExecutors")
    public void testStreamClosesAfterLocalShutdownInputAndFin(Executor executor) throws Throwable {
        Channel server = null;
        Channel channel = null;
        try {
            Streams serverStreams = new Streams();
            Streams clientStreams = new Streams();
            server = QuicTestUtils.newServer(executor, QuicTestUtils.NOOP_HANDLER, serverStreams);
            channel = QuicTestUtils.newClient(executor);
            QuicChannel quicChannel = connect(channel, server);

            QuicStreamChannel clientStream = quicChannel.createStream(QuicStreamType.BIDIRECTIONAL,
                    clientStreams).get();
            QuicStreamChannel serverStream = serverStreams.open(clientStream);

            serverStream.shutdownInput((int) ERROR_CODE).sync();
            serverStream.writeAndFlush(new DefaultQuicStreamFrame(Unpooled.directBuffer(1).writeZero(1), true))
                    .sync();
            // The client answers the STOP_SENDING with a RESET_STREAM (quiche does so on its own).
            assertStopped(clientStreams.next());

            assertThat(serverStream.closeFuture().await(5, TimeUnit.SECONDS))
                    .as("server stream closed after shutdownInput, FIN and the peer's reset").isTrue();

            quicChannel.close().sync();
        } finally {
            QuicTestUtils.closeIfNotNull(channel);
            QuicTestUtils.closeIfNotNull(server);
            shutdown(executor);
        }
    }

    /**
     * violetagg's case: the error code on a unidirectional stream we opened.
     */
    @ParameterizedTest
    @MethodSource("newSslTaskExecutors")
    public void testStopSendingOnUnidirectionalStreamHasErrorCode(Executor executor) throws Throwable {
        Channel server = null;
        Channel channel = null;
        try {
            Streams serverStreams = new Streams();
            Streams clientStreams = new Streams();
            server = QuicTestUtils.newServer(executor, QuicTestUtils.NOOP_HANDLER, serverStreams);
            channel = QuicTestUtils.newClient(executor);
            QuicChannel quicChannel = connect(channel, server);

            QuicStreamChannel clientStream = quicChannel.createStream(QuicStreamType.UNIDIRECTIONAL,
                    clientStreams).get();
            QuicStreamChannel serverStream = serverStreams.open(clientStream);

            serverStream.shutdownInput((int) ERROR_CODE).sync();

            assertStopped(clientStreams.next());
            assertThat(clientStream.isOutputShutdown()).isTrue();

            quicChannel.close().sync();
        } finally {
            QuicTestUtils.closeIfNotNull(channel);
            QuicTestUtils.closeIfNotNull(server);
            shutdown(executor);
        }
    }

    /**
     * The server finished a response and closed its stream channel, but the data is not acknowledged yet when the
     * client stops the stream. quiche (since 0.30.1) keeps a stopped stream until the stop is reported to the
     * application, so if netty never asks about a stream it has no channel for, the stream is never collected and
     * the client never gets its stream credit back.
     */
    @ParameterizedTest
    @MethodSource("newSslTaskExecutors")
    @Timeout(60)
    public void testStopSendingAfterChannelClosedReturnsStreamCredit(Executor executor) throws Throwable {
        runCreditScenario(executor, true);
    }

    /**
     * Control for {@link #testStopSendingAfterChannelClosedReturnsStreamCredit}: the same exchange without loss and
     * without STOP_SENDING gives the credit back.
     */
    @ParameterizedTest
    @MethodSource("newSslTaskExecutors")
    @Timeout(60)
    public void testCompletedStreamsReturnStreamCredit(Executor executor) throws Throwable {
        runCreditScenario(executor, false);
    }

    private static void runCreditScenario(Executor executor, boolean stop) throws Throwable {
        Channel server = null;
        Channel channel = null;
        try (DroppingRelay relay = new DroppingRelay()) {
            BlockingQueue<QuicStreamChannel> closed = new LinkedBlockingQueue<>();
            BlockingQueue<QuicStreamChannel> barriers = new LinkedBlockingQueue<>();
            BlockingQueue<Object> limitChanged = new LinkedBlockingQueue<>();
            ChannelInboundHandlerAdapter responder = new ChannelInboundHandlerAdapter() {
                @Override
                public boolean isSharable() {
                    return true;
                }

                @Override
                public void handlerAdded(ChannelHandlerContext ctx) {
                    QuicStreamChannel stream = (QuicStreamChannel) ctx.channel();
                    if (stream.type() == QuicStreamType.UNIDIRECTIONAL) {
                        barriers.add(stream);
                    }
                }

                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                    ReferenceCountUtil.release(msg);
                }

                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                    if (evt == ChannelInputShutdownReadComplete.INSTANCE &&
                            ((QuicStreamChannel) ctx.channel()).type() == QuicStreamType.BIDIRECTIONAL) {
                        // The response never reaches the client, so it stays unacknowledged.
                        relay.dropToClient = stop;
                        QuicStreamChannel stream = (QuicStreamChannel) ctx.channel();
                        stream.closeFuture().addListener(f -> closed.add(stream));
                        ctx.writeAndFlush(new DefaultQuicStreamFrame(
                                Unpooled.directBuffer(8).writeZero(8), true));
                    }
                    ctx.fireUserEventTriggered(evt);
                }
            };
            server = QuicTestUtils.newServer(QuicTestUtils.newQuicServerBuilder(executor)
                    .initialMaxStreamsBidirectional(2), InsecureQuicTokenHandler.INSTANCE,
                    QuicTestUtils.NOOP_HANDLER, responder);
            relay.start((InetSocketAddress) server.localAddress());
            channel = QuicTestUtils.newClient(executor);
            QuicChannel quicChannel = QuicTestUtils.newQuicChannelBootstrap(channel)
                    .handler(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                            if (evt == QuicStreamLimitChangedEvent.INSTANCE) {
                                limitChanged.add(evt);
                            }
                            ctx.fireUserEventTriggered(evt);
                        }
                    })
                    .streamHandler(new ChannelInboundHandlerAdapter())
                    .remoteAddress(relay.address())
                    .connect()
                    .get();

            for (int i = 0; i < 5; i++) {
                QuicStreamChannel clientStream = createStreamWithin(quicChannel, limitChanged, 8, i);
                clientStream.writeAndFlush(new DefaultQuicStreamFrame(Unpooled.directBuffer(1).writeZero(1), true))
                        .sync();
                QuicStreamChannel serverStream = closed.poll(5, TimeUnit.SECONDS);
                assertThat(serverStream).as("server stream %d closed", i).isNotNull();

                if (stop) {
                    clientStream.shutdownInput((int) ERROR_CODE).sync();
                    // The STOP_SENDING must arrive while the response is still unacknowledged. The data of a
                    // unidirectional stream is sent after it, so once the server has seen that stream, it has also
                    // processed the STOP_SENDING.
                    QuicStreamChannel barrier = quicChannel.createStream(QuicStreamType.UNIDIRECTIONAL,
                            new ChannelInboundHandlerAdapter()).get();
                    barrier.writeAndFlush(new DefaultQuicStreamFrame(Unpooled.directBuffer(1).writeZero(1), true))
                            .sync();
                    assertThat(barriers.poll(5, TimeUnit.SECONDS)).as("barrier stream %d", i).isNotNull();
                    relay.dropToClient = false;
                }
            }

            quicChannel.close().sync();
        } finally {
            QuicTestUtils.closeIfNotNull(channel);
            QuicTestUtils.closeIfNotNull(server);
            shutdown(executor);
        }
    }

    /**
     * Creates a stream, waiting for the server to give stream credit back if there is none left.
     */
    private static QuicStreamChannel createStreamWithin(QuicChannel quicChannel, BlockingQueue<Object> limitChanged,
                                                        int seconds, int index) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        Throwable last = null;
        do {
            // Cleared before the attempt, so a change that comes in while it fails is not missed.
            limitChanged.clear();
            Future<QuicStreamChannel> f = quicChannel.createStream(QuicStreamType.BIDIRECTIONAL,
                    new ChannelInboundHandlerAdapter()).await();
            if (f.isSuccess()) {
                return f.getNow();
            }
            last = f.cause();
        } while (limitChanged.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS) != null);
        throw new AssertionError("could not open stream " + index + " within " + seconds +
                "s: the server did not give the stream credit back", last);
    }

    private static QuicChannel connect(Channel channel, Channel server) throws Exception {
        return QuicTestUtils.newQuicChannelBootstrap(channel)
                .handler(new ChannelInboundHandlerAdapter())
                .streamHandler(new ChannelInboundHandlerAdapter())
                .remoteAddress(server.localAddress())
                .connect()
                .get();
    }

    private static void assertStopped(Object event) {
        assertThat(event).as("QuicStreamStoppedEvent").isInstanceOf(QuicStreamStoppedEvent.class);
        assertThat(((QuicStreamStoppedEvent) event).applicationProtocolCode()).isEqualTo(ERROR_CODE);
    }

    /**
     * Records the streams it is added to and their events.
     */
    private static final class Streams extends ChannelInboundHandlerAdapter {
        private final BlockingQueue<QuicStreamChannel> streams = new LinkedBlockingQueue<>();
        final BlockingQueue<Object> events = new LinkedBlockingQueue<>();

        @Override
        public boolean isSharable() {
            return true;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            streams.add((QuicStreamChannel) ctx.channel());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ReferenceCountUtil.release(msg);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof QuicStreamStoppedEvent || evt == ChannelInputShutdownReadComplete.INSTANCE) {
                events.add(evt);
            }
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            events.add(cause);
        }

        QuicStreamChannel open(QuicStreamChannel clientStream) throws InterruptedException {
            clientStream.writeAndFlush(Unpooled.buffer().writeByte(0)).sync();
            QuicStreamChannel stream = streams.poll(5, TimeUnit.SECONDS);
            assertThat(stream).isNotNull();
            return stream;
        }

        Object next() throws InterruptedException {
            return events.poll(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Relays UDP between one client and the server, and drops what the server sends while {@link #dropToClient}.
     */
    private static final class DroppingRelay implements AutoCloseable {
        private final DatagramSocket socket;
        private volatile InetSocketAddress server;
        private volatile InetSocketAddress client;
        volatile boolean dropToClient;
        private volatile boolean closed;
        private Thread thread;

        DroppingRelay() throws Exception {
            socket = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            socket.setSoTimeout(100);
        }

        InetSocketAddress address() {
            return (InetSocketAddress) socket.getLocalSocketAddress();
        }

        void start(InetSocketAddress server) {
            this.server = new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getPort());
            thread = new Thread(this::run, "dropping-relay");
            thread.setDaemon(true);
            thread.start();
        }

        private void run() {
            byte[] buf = new byte[65536];
            while (!closed) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                try {
                    socket.receive(packet);
                    InetSocketAddress from = (InetSocketAddress) packet.getSocketAddress();
                    if (from.equals(server)) {
                        InetSocketAddress to = client;
                        if (!dropToClient && to != null) {
                            socket.send(new DatagramPacket(packet.getData(), packet.getOffset(),
                                    packet.getLength(), to));
                        }
                    } else {
                        client = from;
                        socket.send(new DatagramPacket(packet.getData(), packet.getOffset(),
                                packet.getLength(), server));
                    }
                } catch (SocketTimeoutException ignore) {
                    // check closed
                } catch (Exception e) {
                    if (!closed) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            closed = true;
            if (thread != null) {
                thread.join(1000);
            }
            socket.close();
        }
    }
}
