package org.helios;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.gateway.Gateway;
import org.helios.gateway.GatewayHandler;
import org.helios.gateway.GatewayHandlerFactory;
import org.helios.infra.MessageTypes;
import org.helios.service.Service;
import org.helios.service.ServiceHandler;
import org.helios.util.RingBufferPool;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

public class HeliosTest
{
    private static final int EMBEDDED_INPUT_STREAM_ID = 10;
    private static final int EMBEDDED_OUTPUT_STREAM_ID = 11;

    private static final String INPUT_CHANNEL = "aeron:udp?endpoint=localhost:40123";
    private static final String OUTPUT_CHANNEL = "aeron:udp?endpoint=localhost:40124";
    private static final int INPUT_STREAM_ID = 10;
    private static final int OUTPUT_STREAM_ID = 11;

    private static final int NUM_GATEWAYS = 2; //2

    @Test
    public void shouldAssociateOneEmbeddedServiceWithOneEmbeddedGateway() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(1);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(1);

            final AeronStream embeddedInputStream = helios.newEmbeddedStream(EMBEDDED_INPUT_STREAM_ID);
            final AeronStream embeddedOutputStream = helios.newEmbeddedStream(EMBEDDED_OUTPUT_STREAM_ID);

            helios.addService(NullServiceHandler::new, s2gAssociationLatch::countDown, ()->{},
                embeddedInputStream, embeddedOutputStream);
            helios.addGateway(NullGatewayHandler::new, g2sAssociationLatch::countDown, ()->{},
                embeddedInputStream, embeddedOutputStream);

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();
        }

        assertTrue(true);
    }

    @Test
    public void shouldAssociateOneServiceWithOneGateway() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(1);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(1);

            final AeronStream svcInputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream svcOutputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            helios.addService(NullServiceHandler::new)
                .availableAssociationHandler(s2gAssociationLatch::countDown)
                .addEndPoint(svcInputStream, svcOutputStream);

            final AeronStream gwOutputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream gwInputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            helios.addGateway(NullGatewayHandler::new)
                .availableAssociationHandler(g2sAssociationLatch::countDown)
                .addEndPoint(gwOutputStream, gwInputStream);

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();
        }

        assertTrue(true);
    }

    @Test
    public void shouldAssociateOneEmbeddedServiceWithMultipleEmbeddedGateways() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(NUM_GATEWAYS);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(NUM_GATEWAYS);

            final AeronStream embeddedInputStream = helios.newEmbeddedStream(EMBEDDED_INPUT_STREAM_ID);

            final Service<NullServiceHandler> svc = helios.addService(NullServiceHandler::new,
                s2gAssociationLatch::countDown, () -> {});

            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                final AeronStream embeddedOutputStream = helios.newEmbeddedStream(EMBEDDED_OUTPUT_STREAM_ID+i);

                svc.addEndPoint(embeddedInputStream, embeddedOutputStream);

                helios.addGateway(NullGatewayHandler::new, g2sAssociationLatch::countDown, () -> {},
                    embeddedInputStream, embeddedOutputStream);
            }

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();
        }

        assertTrue(true);
    }

    @Test
    public void shouldAssociateOneServiceWithMultipleGateways() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(NUM_GATEWAYS);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(NUM_GATEWAYS);

            final Service<NullServiceHandler> svc = helios.addService(NullServiceHandler::new)
                .availableAssociationHandler(s2gAssociationLatch::countDown);

            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                final AeronStream svcInputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
                final AeronStream svcOutputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID+i);
                svc.addEndPoint(svcInputStream, svcOutputStream);

                final AeronStream gwOutputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
                final AeronStream gwInputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID+i);
                helios.addGateway(NullGatewayHandler::new, g2sAssociationLatch::countDown, () -> {},
                    gwOutputStream, gwInputStream);
            }

            assertTrue(helios.numServiceSubscriptions() == NUM_GATEWAYS);
            assertTrue(helios.numGatewaySubscriptions() == NUM_GATEWAYS);

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();
        }

        assertTrue(true);
    }

    @Test
    public void shouldPingOneServiceWithOneGateway() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(1);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(1);
            final CountDownLatch pingLatch = new CountDownLatch(1);

            final AeronStream svcInputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream svcOutputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            final Service<PingServiceHandler> svc = helios.addService(PingServiceHandler::new)
                .availableAssociationHandler(s2gAssociationLatch::countDown)
                .addEndPoint(svcInputStream, svcOutputStream);

            final int gatewayId = 1;
            svc.handler().addOutputStream(gatewayId, svcOutputStream);// FIXME: define Helios Gateway-Service (HGS) protocol

            final AeronStream gwOutputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream gwInputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            final Gateway<PingGatewayHandler> gw =
                helios.addGateway(
                    (outputBufferPool) -> new PingGatewayHandler(gatewayId,
                        outputBufferPool, gwOutputStream,
                        (msgTypeId, buffer, index, length) -> {
                            if (msgTypeId == MessageTypes.APPLICATION_MSG_ID)
                            {
                                // APPLICATION message type, ignore */
                                assertTrue(buffer.getInt(index) == gatewayId);
                                assertTrue(length == PingGatewayHandler.MESSAGE_LENGTH);
                                pingLatch.countDown();
                            }
                            /*else
                            {
                                // ADMINISTRATIVE message type, ignore
                            }*/
                        })
                );
            gw.availableAssociationHandler(g2sAssociationLatch::countDown);
            gw.addEndPoint(gwOutputStream, gwInputStream);

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();

            gw.handler().sendPing();

            pingLatch.await();
        }

        assertTrue(true);
    }

    @Test
    public void shouldPingOneEmbeddedServiceWithOneEmbeddedGateway() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(1);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(1);
            final CountDownLatch pingLatch = new CountDownLatch(1);

            final AeronStream embeddedInputStream = helios.newEmbeddedStream(EMBEDDED_INPUT_STREAM_ID);
            final AeronStream embeddedOutputStream = helios.newEmbeddedStream(EMBEDDED_OUTPUT_STREAM_ID);

            final Service<PingServiceHandler> svc = helios.addService(PingServiceHandler::new,
                s2gAssociationLatch::countDown, () -> {},
                embeddedInputStream, embeddedOutputStream);

            final int gatewayId = 1;
            svc.handler().addOutputStream(gatewayId, embeddedOutputStream);// FIXME: define Helios Gateway-Service (HGS) protocol

            final Gateway<PingGatewayHandler> gw =
                helios.addGateway(
                    (outputBufferPool) -> new PingGatewayHandler(gatewayId,
                        outputBufferPool, embeddedInputStream,
                        (msgTypeId, buffer, index, length) -> {
                            if (msgTypeId == MessageTypes.APPLICATION_MSG_ID)
                            {
                                // APPLICATION message type, ignore */
                                assertTrue(buffer.getInt(index) == gatewayId);
                                assertTrue(length == PingGatewayHandler.MESSAGE_LENGTH);
                                pingLatch.countDown();
                            }
                            /*else
                            {
                                // ADMINISTRATIVE message type, ignore
                            }*/
                        })
                );
            gw.availableAssociationHandler(g2sAssociationLatch::countDown);
            gw.addEndPoint(embeddedInputStream, embeddedOutputStream);

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();

            gw.handler().sendPing();

            pingLatch.await();
        }

        assertTrue(true);
    }

    @Test
    public void shouldPingOneServiceWithMultipleGateways() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(NUM_GATEWAYS);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(NUM_GATEWAYS);
            final CountDownLatch pingLatch = new CountDownLatch(NUM_GATEWAYS);

            final Service<PingServiceHandler> svc = helios.addService(PingServiceHandler::new,
                s2gAssociationLatch::countDown, () -> {});

            final List<Gateway<PingGatewayHandler>> gateways = new ArrayList<>();
            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                final AeronStream svcInputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID+i);
                final AeronStream svcOutputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID+NUM_GATEWAYS+i);

                svc.addEndPoint(svcInputStream, svcOutputStream);

                svc.handler().addOutputStream(i, svcOutputStream); // FIXME: define Helios Gateway-Service (HGS) protocol

                final AeronStream gwOutputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID+i);
                final AeronStream gwInputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID+NUM_GATEWAYS+i);

                final Gateway<PingGatewayHandler> gw = helios.addGateway(
                    new PingGatewayHandlerFactory(i, gwOutputStream, new PingMessageHandler(i, pingLatch)));
                gw.availableAssociationHandler(g2sAssociationLatch::countDown);
                gw.addEndPoint(gwOutputStream, gwInputStream);

                gateways.add(gw);
            }

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();

            gateways.forEach((gw) -> gw.handler().sendPing());

            pingLatch.await();
        }

        assertTrue(true);
    }

    /*@Test
    public void shouldPingOneEmbeddedServiceWithMultipleEmbeddedGateways() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(NUM_GATEWAYS);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(NUM_GATEWAYS);
            final CountDownLatch pingLatch = new CountDownLatch(NUM_GATEWAYS);

            final AeronStream embeddedInputStream = helios.newEmbeddedStream(EMBEDDED_INPUT_STREAM_ID);

            final Service<PingServiceHandler> svc = helios.addService(PingServiceHandler::new,
                s2gAssociationLatch::countDown, () -> {});

            final List<Gateway<PingGatewayHandler>> gateways = new ArrayList<>();
            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                final AeronStream embeddedOutputStream = helios.newEmbeddedStream(EMBEDDED_OUTPUT_STREAM_ID+i);

                svc.addEndPoint(embeddedInputStream, embeddedOutputStream);

                final int gatewayId = i;
                final Gateway<PingGatewayHandler> gw = helios.addGateway(
                    (outputBufferPool) -> new PingGatewayHandler(outputBufferPool,
                        (msgTypeId, buffer, index, length) -> {
                            if (msgTypeId == MessageTypes.APPLICATION_MSG_ID)
                            {
                                System.out.println("PingGatewayHandler::onMessage buffer.getInt(index)=" + buffer.getInt(index) + " gatewayId=" + gatewayId);
                                boolean b = buffer.getInt(index) == gatewayId;
                                System.out.println("PingGatewayHandler::onMessage b=" + b);
                                // APPLICATION message type, ignore
                                assertTrue(buffer.getInt(index) == gatewayId);
                                assertTrue(length == PingGatewayHandler.MESSAGE_LENGTH);
                                pingLatch.countDown();
                            }
                            //else
                            //{
                                // ADMINISTRATIVE message type, ignore
                            //}
                        },
                        gatewayId));
                gw.availableAssociationHandler(g2sAssociationLatch::countDown);
                gw.addEndPoint(embeddedInputStream, embeddedOutputStream);

                gateways.add(gw);
            }

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();

            gateways.forEach((gw) -> gw.handler().sendPing());

            pingLatch.await();
        }

        assertTrue(true);
    }*/

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenOnlyContextIsNull()
    {
        try(final Helios helios = new Helios(null))
        {
            helios.start();
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenDriverIsNull() throws Exception
    {
        try(final Helios helios = new Helios(new HeliosContext(), null))
        {
            helios.start();
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenContextIsNull() throws Exception
    {
        try(final HeliosDriver driver = new HeliosDriver(new HeliosContext());
            final Helios helios = new Helios(null, driver))
        {
            helios.start();
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenErrorHandlerIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.errorHandler(null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenStreamChannelIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.newStream(null, 0);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenServiceRequestStreamIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addService((outputBuffers) -> null).addEndPoint(null, helios.newStream(OUTPUT_CHANNEL, 0));

        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenServiceResponseStreamIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addService((outputBuffers) -> null).addEndPoint(helios.newStream(INPUT_CHANNEL, 0), null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenServiceFactoryIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addService(null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayRequestStreamIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addGateway((outputBuffers) -> null).addEndPoint(null, helios.newStream(INPUT_CHANNEL, 0));
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayResponseStreamIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addGateway((outputBuffers) -> null).addEndPoint(helios.newStream(INPUT_CHANNEL, 0), null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayFactoryIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addGateway(null);
        }
    }

    private class NullServiceHandler implements ServiceHandler
    {
        NullServiceHandler(final RingBufferPool ringBufferPool)
        {
            Objects.nonNull(ringBufferPool);
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
        }

        @Override
        public void close() throws Exception
        {
        }
    }

    private class NullGatewayHandler implements GatewayHandler
    {
        NullGatewayHandler(final RingBufferPool ringBufferPool)
        {
            Objects.nonNull(ringBufferPool);
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
        }

        @Override
        public void close() throws Exception
        {
        }
    }

    private class PingServiceHandler implements ServiceHandler
    {
        private final RingBufferPool ringBufferPool;
        private final IdleStrategy idleStrategy;
        private Int2ObjectHashMap<AeronStream> gatewayId2StreamMap; // FIXME: temporary workaround, use HGS protocol

        PingServiceHandler(final RingBufferPool ringBufferPool)
        {
            this.ringBufferPool = ringBufferPool;
            this.idleStrategy = new BusySpinIdleStrategy();
            this.gatewayId2StreamMap = new Int2ObjectHashMap<>();
        }

        void addOutputStream(final int gatewayId, final AeronStream outputStream)
        {
            gatewayId2StreamMap.put(gatewayId, outputStream);
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            if (msgTypeId == MessageTypes.APPLICATION_MSG_ID)
            {
                final int gatewayId = buffer.getInt(index); // FIXME: temporary workaround, use HGS protocol header

                final AeronStream outputStream = gatewayId2StreamMap.get(gatewayId);
                final RingBuffer outputBuffer = ringBufferPool.getOutputRingBuffer(outputStream);// FIXME: refactoring to avoid this API

                /* APPLICATION message type, send ping message back */
                while (!outputBuffer.write(msgTypeId, buffer, index, length))
                {
                    idleStrategy.idle(0);
                }
            }
            /*else
            {
                // ADMINISTRATIVE message type, ignore
            }*/
        }

        @Override
        public void close() throws Exception
        {
        }
    }

    private class PingGatewayHandler implements GatewayHandler
    {
        private static final int MESSAGE_LENGTH = 8;

        private final int gatewayId;
        private final RingBufferPool ringBufferPool;
        private final AeronStream gwOutputStream;
        private final MessageHandler delegate;
        private final IdleStrategy idleStrategy;
        private final UnsafeBuffer echoBuffer;

        PingGatewayHandler(final int gatewayId, final RingBufferPool ringBufferPool, final AeronStream gwOutputStream,
            final MessageHandler delegate)
        {
            this.gatewayId = gatewayId;
            this.ringBufferPool = ringBufferPool;
            this.gwOutputStream = gwOutputStream;
            this.delegate = delegate;
            this.idleStrategy = new BusySpinIdleStrategy();
            this.echoBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));
        }

        void sendPing()
        {
            final RingBuffer outputBuffer = ringBufferPool.getOutputRingBuffer(gwOutputStream); // FIXME: refactoring to avoid this API

            echoBuffer.putInt(0, gatewayId); // FIXME: temporary workaround, use HGS protocol header

            while (!outputBuffer.write(MessageTypes.APPLICATION_MSG_ID, echoBuffer, 0, MESSAGE_LENGTH))
            {
                idleStrategy.idle(0);
            }
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            delegate.onMessage(msgTypeId, buffer, index, length);
        }

        @Override
        public void close() throws Exception
        {
        }
    }

    private class PingGatewayHandlerFactory implements GatewayHandlerFactory<PingGatewayHandler>
    {
        private final int gatewayId;
        private final AeronStream gwOutputStream;
        private final MessageHandler delegate;

        PingGatewayHandlerFactory(final int gatewayId, final AeronStream gwOutputStream, final MessageHandler delegate)
        {
            this.gatewayId = gatewayId;
            this.gwOutputStream = gwOutputStream;
            this.delegate = delegate;
        }

        @Override
        public PingGatewayHandler createGatewayHandler(final RingBufferPool ringBufferPool)
        {
            return new PingGatewayHandler(gatewayId, ringBufferPool, gwOutputStream, delegate);
        }
    }

    private class PingMessageHandler implements MessageHandler
    {
        private final int gatewayId;
        private final CountDownLatch pingLatch;

        PingMessageHandler(final int gatewayId, final CountDownLatch pingLatch)
        {
            this.gatewayId = gatewayId;
            this.pingLatch = pingLatch;
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            if (msgTypeId == MessageTypes.APPLICATION_MSG_ID)
            {
                // APPLICATION message type: check echo parameters
                assertTrue(buffer.getInt(index) == gatewayId);
                assertTrue(length == PingGatewayHandler.MESSAGE_LENGTH);
                pingLatch.countDown();
            }
        }
    }
}
