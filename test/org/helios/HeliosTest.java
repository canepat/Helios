package org.helios;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.gateway.Gateway;
import org.helios.infra.MessageTypes;
import org.helios.service.ServiceHandler;
import org.helios.gateway.GatewayHandler;
import org.junit.Test;

import java.nio.ByteBuffer;
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

    private static final int NUM_GATEWAYS = 2;

    @Test
    public void shouldAssociateOneEmbeddedServiceWithOneEmbeddedGateway() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch associationLatch = new CountDownLatch(1);

            helios.addEmbeddedService(EMBEDDED_INPUT_STREAM_ID, EMBEDDED_OUTPUT_STREAM_ID,
                (outputBuffers) -> new NullServiceHandler());
            helios.addEmbeddedGateway(EMBEDDED_INPUT_STREAM_ID, EMBEDDED_OUTPUT_STREAM_ID,
                (outputBuffers) -> new NullGatewayHandler())
                .availableAssociationHandler(associationLatch::countDown);

            helios.start();

            associationLatch.await();
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
            helios.addService(svcInputStream, svcOutputStream, (outputBuffers) -> new NullServiceHandler())
                .availableAssociationHandler(s2gAssociationLatch::countDown);

            final AeronStream gwOutputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream gwInputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            helios.addGateway(gwOutputStream, gwInputStream, (outputBuffers) -> new NullGatewayHandler())
                .availableAssociationHandler(g2sAssociationLatch::countDown);

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
            final CountDownLatch associationLatch = new CountDownLatch(NUM_GATEWAYS);

            helios.addEmbeddedService(EMBEDDED_INPUT_STREAM_ID, EMBEDDED_OUTPUT_STREAM_ID,
                (outputBuffers) -> new NullServiceHandler());

            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                helios.addEmbeddedGateway(EMBEDDED_INPUT_STREAM_ID, EMBEDDED_OUTPUT_STREAM_ID,
                    (outputBuffers) -> new NullGatewayHandler())
                    .availableAssociationHandler(associationLatch::countDown);
            }

            helios.start();

            associationLatch.await();
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
            helios.addService(svcInputStream, svcOutputStream,
                (outputBuffers) -> new PingServiceHandler(outputBuffers[0]))
                .availableAssociationHandler(s2gAssociationLatch::countDown);

            final int gatewayId = 1;
            final AeronStream gwOutputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream gwInputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            final Gateway<PingGatewayHandler> gw =
                helios.addGateway(gwOutputStream, gwInputStream,
                    (outputBuffers) -> new PingGatewayHandler(outputBuffers[0],
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
                        }
                        , gatewayId))
                    .availableAssociationHandler(g2sAssociationLatch::countDown);

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
            final CountDownLatch associationLatch = new CountDownLatch(1);
            final CountDownLatch pingLatch = new CountDownLatch(1);

            helios.addEmbeddedService(EMBEDDED_INPUT_STREAM_ID, EMBEDDED_OUTPUT_STREAM_ID,
                (outputBuffers) -> new PingServiceHandler(outputBuffers[0]));

            final int gatewayId = 1;
            final Gateway<PingGatewayHandler> gw =
                helios.addEmbeddedGateway(
                    EMBEDDED_INPUT_STREAM_ID,
                    EMBEDDED_OUTPUT_STREAM_ID,
                    (outputBuffers) -> new PingGatewayHandler(outputBuffers[0],
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
                        }
                        , gatewayId));

            gw.availableAssociationHandler(associationLatch::countDown);

            helios.start();

            associationLatch.await();

            gw.handler().sendPing();

            pingLatch.await();
        }

        assertTrue(true);
    }

    /*@Test
    public void shouldPingOneEmbeddedServiceWithMultipleEmbeddedGateways() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch associationLatch = new CountDownLatch(NUM_GATEWAYS);
            final CountDownLatch pingLatch = new CountDownLatch(NUM_GATEWAYS);

            helios.addEmbeddedService(EMBEDDED_INPUT_STREAM_ID, EMBEDDED_OUTPUT_STREAM_ID,
                (outputBuffers) -> new PingServiceHandler(outputBuffers[0]));

            final List<Gateway<PingGatewayHandler>> gateways = new ArrayList<>();
            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                final int gatewayId = i;

                final Gateway<PingGatewayHandler> gw =
                    helios.addEmbeddedGateway(
                        EMBEDDED_INPUT_STREAM_ID,
                        EMBEDDED_OUTPUT_STREAM_ID,
                        (outputBuffers) -> new PingGatewayHandler(outputBuffers[0],
                            (msgTypeId, buffer, index, length) -> {
                                if (msgTypeId == MessageTypes.APPLICATION_MSG_ID)
                                {
                                    // APPLICATION message type, ignore
                                    assertTrue(buffer.getInt(index) == gatewayId);
                                    assertTrue(length == PingGatewayHandler.MESSAGE_LENGTH);
                                    pingLatch.countDown();
                                }
                                else
                                {
                                    // ADMINISTRATIVE message type, ignore
                                }
                            }
                            , gatewayId));

                gw.availableAssociationHandler(associationLatch::countDown);
                gateways.add(gw);
            }

            helios.start();

            associationLatch.await();

            gateways.forEach((gw) -> gw.handler().sendPing());

            pingLatch.await();
        }

        assertTrue(true);
    }*/

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenOnlyContextIsNull()
    {
        new Helios(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenDriverIsNull() throws Exception
    {
        new Helios(new HeliosContext(), null);
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
    public void shouldThrowExceptionWhenEmbeddedServiceFactoryIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addEmbeddedService(0, 0, null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenServiceRequestStreamIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addService(null, helios.newStream("", 0), (outputBuffers) -> null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenServiceResponseStreamIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addService(helios.newStream("", 0), null, (outputBuffers) -> null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenServiceFactoryIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addService(helios.newStream("", 0), helios.newStream("", 0), null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenEmbeddedGatewayFactoryIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addEmbeddedGateway(0, 0, null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayRequestStreamIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addGateway(null, helios.newStream("", 0), (outputBuffers) -> null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayResponseStreamIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addGateway(helios.newStream("", 0), null, (outputBuffers) -> null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayFactoryIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addGateway(helios.newStream("", 0), helios.newStream("", 0), null);
        }
    }

    private class NullServiceHandler implements ServiceHandler
    {
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
        private final RingBuffer outputBuffer;
        private final IdleStrategy idleStrategy;

        PingServiceHandler(final RingBuffer outputBuffer)
        {
            this.outputBuffer = outputBuffer;
            this.idleStrategy = new BusySpinIdleStrategy();
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            if (msgTypeId == MessageTypes.APPLICATION_MSG_ID)
            {
                /* APPLICATION message type, send ping message back */
                while (!outputBuffer.write(msgTypeId, buffer, index, length))
                {
                    idleStrategy.idle(0);
                }
            }
            else
            {
                /* ADMINISTRATIVE message type, ignore */
            }
        }

        @Override
        public void close() throws Exception
        {
        }
    }

    private class PingGatewayHandler implements GatewayHandler
    {
        private static final int MESSAGE_LENGTH = 8;

        private final RingBuffer outputBuffer;
        private final MessageHandler delegate;
        private final int gatewayId;
        private final IdleStrategy idleStrategy;
        private final UnsafeBuffer echoBuffer;

        PingGatewayHandler(final RingBuffer outputBuffer, final MessageHandler delegate, final int gatewayId)
        {
            this.outputBuffer = outputBuffer;
            this.delegate = delegate;
            this.gatewayId = gatewayId;
            this.idleStrategy = new BusySpinIdleStrategy();
            this.echoBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));
        }

        public void sendPing()
        {
            echoBuffer.putInt(0, gatewayId);

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
}
