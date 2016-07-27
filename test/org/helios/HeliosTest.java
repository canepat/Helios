package org.helios;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.MessageTypes;
import org.helios.service.ServiceHandler;
import org.helios.gateway.GatewayHandler;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

public class HeliosTest
{
    private static final int SVC_INPUT_STREAM_ID = 10;
    private static final int SVC_OUTPUT_STREAM_ID = 11;
    private static final int NUM_GATEWAYS = 1;

    @Test
    public void shouldAssociateOneServiceWithOneGateway() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch associationLatch = new CountDownLatch(1);

            helios.addEmbeddedService(SVC_INPUT_STREAM_ID, SVC_OUTPUT_STREAM_ID,
                (outputBuffers) -> new NullServiceHandler());
            helios.addEmbeddedGateway(SVC_INPUT_STREAM_ID, SVC_OUTPUT_STREAM_ID,
                (outputBuffers) -> new NullGatewayHandler())
                .availableAssociationHandler(associationLatch::countDown);

            helios.start();

            associationLatch.await();
        }

        assertTrue(true);
    }

    @Test
    public void shouldAssociateOneServiceWithMultipleGateways() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch associationLatch = new CountDownLatch(NUM_GATEWAYS);

            helios.addEmbeddedService(SVC_INPUT_STREAM_ID, SVC_OUTPUT_STREAM_ID,
                (outputBuffers) -> new NullServiceHandler());

            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                helios.addEmbeddedGateway(SVC_INPUT_STREAM_ID, SVC_OUTPUT_STREAM_ID,
                    (outputBuffers) -> new NullGatewayHandler())
                    .availableAssociationHandler(associationLatch::countDown);
            }

            helios.start();

            associationLatch.await();
        }

        assertTrue(true);
    }

    /*@Test
    public void shouldPingOneServiceWithMultipleGateways() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch associationLatch = new CountDownLatch(NUM_GATEWAYS);
            final CountDownLatch pingLatch = new CountDownLatch(NUM_GATEWAYS);

            helios.addEmbeddedService(SVC_INPUT_STREAM_ID, SVC_OUTPUT_STREAM_ID,
                (outputBuffers) -> new PingServiceHandler(outputBuffers[0]));

            final List<Gateway<PingGatewayHandler>> gateways = new ArrayList<>();
            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                final int gatewayId = i;

                final Gateway<PingGatewayHandler> gw =
                    helios.addEmbeddedGateway(
                        SVC_INPUT_STREAM_ID,
                        SVC_OUTPUT_STREAM_ID,
                        (outputBuffers) -> new PingGatewayHandler(outputBuffers[0],
                            (msgTypeId, buffer, index, length) -> {
                                assertTrue(buffer.getInt(index) == gatewayId);
                                assertTrue(length == PingGatewayHandler.MESSAGE_LENGTH);
                                System.out.println("index=" + index + " length=" + length);
                                System.out.println("gatewayId=" + gatewayId + " buffer.getInt(index)=" + buffer.getInt(index));
                                pingLatch.countDown();
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
            while (!outputBuffer.write(MessageTypes.APPLICATION_MSG_ID, buffer, index, length))
            {
                idleStrategy.idle(0);
            }
        }

        @Override
        public void close() throws Exception
        {
        }
    }

    private class PingGatewayHandler implements GatewayHandler
    {
        private static final int MESSAGE_LENGTH = 64;

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
