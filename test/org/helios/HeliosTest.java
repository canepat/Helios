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
import org.helios.mmb.DataMessage;
import org.helios.mmb.sbe.ComponentDecoder;
import org.helios.mmb.sbe.ComponentType;
import org.helios.mmb.sbe.DataDecoder;
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
    private static final String INPUT_CHANNEL = "aeron:udp?endpoint=localhost:40123";
    private static final String OUTPUT_CHANNEL = "aeron:udp?endpoint=localhost:40124";
    private static final int INPUT_STREAM_ID = 10;
    private static final int OUTPUT_STREAM_ID = 11;

    private static final int NUM_GATEWAYS = 2; //2

    @Test
    public void shouldAssociateOneServiceWithOneGatewayIPC() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(1);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(1);

            final AeronStream ipcInputStream = helios.newIpcStream(INPUT_STREAM_ID);
            final AeronStream ipcOutputStream = helios.newIpcStream(OUTPUT_STREAM_ID);

            helios.addService(NullServiceHandler::new, s2gAssociationLatch::countDown, ()->{},
                ipcInputStream, ipcOutputStream);
            helios.addGateway(NullGatewayHandler::new, g2sAssociationLatch::countDown, ()->{},
                ipcInputStream, ipcOutputStream);

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
            helios.addService(NullServiceHandler::new, svcInputStream)
                .availableAssociationHandler(s2gAssociationLatch::countDown)
                .addEndPoint(svcOutputStream);

            final AeronStream gwOutputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream gwInputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            helios.addGateway()
                .availableAssociationHandler(g2sAssociationLatch::countDown)
                .addEndPoint(gwOutputStream, gwInputStream, NullGatewayHandler::new);

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();
        }

        assertTrue(true);
    }

    @Test
    public void shouldAssociateOneServiceWithMultipleGatewaysIPC() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(NUM_GATEWAYS);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(NUM_GATEWAYS);

            final AeronStream ipcInputStream = helios.newIpcStream(INPUT_STREAM_ID);

            final Service<NullServiceHandler> svc = helios.addService(NullServiceHandler::new,
                ipcInputStream, s2gAssociationLatch::countDown, () -> {});

            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                final AeronStream ipcOutputStream = helios.newIpcStream(OUTPUT_STREAM_ID+i);

                svc.addEndPoint(ipcOutputStream);

                helios.addGateway(NullGatewayHandler::new, g2sAssociationLatch::countDown, () -> {},
                    ipcInputStream, ipcOutputStream);
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

            final AeronStream svcInputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream gwOutputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);

            final Service<NullServiceHandler> svc = helios.addService(NullServiceHandler::new, svcInputStream)
                .availableAssociationHandler(s2gAssociationLatch::countDown);

            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                final AeronStream svcOutputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID+NUM_GATEWAYS+i);
                svc.addEndPoint(svcOutputStream);

                final AeronStream gwInputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID+NUM_GATEWAYS+i);
                helios.addGateway(NullGatewayHandler::new, g2sAssociationLatch::countDown, () -> {},
                    gwOutputStream, gwInputStream);
            }

            assertTrue(helios.numServiceSubscriptions() == 1);
            assertTrue(helios.numGatewaySubscriptions() == NUM_GATEWAYS);

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();
        }

        assertTrue(true);
    }

    @Test
    public void shouldPingOneServiceWithOneGatewayIPC() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(1);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(1);
            final CountDownLatch pingLatch = new CountDownLatch(1);

            final AeronStream svcInputStream = helios.newIpcStream(INPUT_STREAM_ID);
            final AeronStream svcOutputStream = helios.newIpcStream(OUTPUT_STREAM_ID);

            final Service<PingServiceHandler> svc = helios.addService(PingServiceHandler::new,
                s2gAssociationLatch::countDown, () -> {},
                svcInputStream, svcOutputStream);

            final int gatewayId = 1;
            svc.handler().addOutputStream(gatewayId, svcOutputStream);// FIXME: define Helios Gateway-Service (HGS) protocol

            final AeronStream gwOutputStream = helios.newIpcStream(INPUT_STREAM_ID);
            final AeronStream gwInputStream = helios.newIpcStream(OUTPUT_STREAM_ID);
            final Gateway<PingGatewayHandler> gw = helios.addGateway();
            gw.availableAssociationHandler(g2sAssociationLatch::countDown);
            final PingGatewayHandler pingHandler = gw.addEndPoint(gwOutputStream, gwInputStream,
                (outputBufferPool) -> new PingGatewayHandler(gatewayId,
                    outputBufferPool, gwOutputStream,
                    (msgTypeId, buffer, index, length) -> {
                        if (msgTypeId == MessageTypes.APPLICATION_MSG_ID)
                        {
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

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();

            pingHandler.sendPing();

            pingLatch.await();
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
            final Service<PingServiceHandler> svc = helios.addService(PingServiceHandler::new, svcInputStream)
                .availableAssociationHandler(s2gAssociationLatch::countDown)
                .addEndPoint(svcOutputStream);

            final int gatewayId = 1;
            svc.handler().addOutputStream(gatewayId, svcOutputStream);// FIXME: define Helios Gateway-Service (HGS) protocol

            final AeronStream gwOutputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream gwInputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            final Gateway<PingGatewayHandler> gw = helios.addGateway();
            gw.availableAssociationHandler(g2sAssociationLatch::countDown);
            final PingGatewayHandler pingHandler = gw.addEndPoint(gwOutputStream, gwInputStream,
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

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();

            pingHandler.sendPing();

            pingLatch.await();
        }

        assertTrue(true);
    }

    @Test
    public void shouldPingOneServiceWithMultipleGatewaysIPC() throws Exception
    {
        try(final Helios helios = new Helios())
        {
            final CountDownLatch s2gAssociationLatch = new CountDownLatch(NUM_GATEWAYS);
            final CountDownLatch g2sAssociationLatch = new CountDownLatch(NUM_GATEWAYS);
            final CountDownLatch pingLatch = new CountDownLatch(NUM_GATEWAYS);

            final AeronStream ipcInputStream = helios.newIpcStream(INPUT_STREAM_ID);

            final Service<PingServiceHandler> svc = helios.addService(PingServiceHandler::new, ipcInputStream,
                s2gAssociationLatch::countDown, () -> {});

            final List<PingGatewayHandler> gatewayHandlers = new ArrayList<>();
            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                final AeronStream ipcOutputStream = helios.newIpcStream(OUTPUT_STREAM_ID+i);

                svc.addEndPoint(ipcOutputStream);

                svc.handler().addOutputStream(i, ipcOutputStream); // FIXME: define Helios Gateway-Service (HGS) protocol

                final Gateway<PingGatewayHandler> gw = helios.addGateway();
                gw.availableAssociationHandler(g2sAssociationLatch::countDown);
                final PingGatewayHandler gwHandler = gw.addEndPoint(ipcInputStream, ipcOutputStream,
                    new PingGatewayHandlerFactory(i, ipcInputStream, new PingMessageHandler(i, pingLatch)));

                gatewayHandlers.add(gwHandler);
            }

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();

            gatewayHandlers.forEach(PingGatewayHandler::sendPing);

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

            final AeronStream svcInputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);

            final Service<PingServiceHandler> svc = helios.addService(PingServiceHandler::new, svcInputStream,
                s2gAssociationLatch::countDown, () -> {});

            final List<PingGatewayHandler> gatewayHandlers = new ArrayList<>();
            for (int i = 0; i < NUM_GATEWAYS; i++)
            {
                final AeronStream svcOutputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID+i);

                svc.addEndPoint(svcOutputStream);

                svc.handler().addOutputStream(i, svcOutputStream); // FIXME: define Helios Gateway-Service (HGS) protocol

                final AeronStream gwInputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID+i);
                final AeronStream gwOutputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);

                final Gateway<PingGatewayHandler> gw = helios.addGateway();
                gw.availableAssociationHandler(g2sAssociationLatch::countDown);
                final PingGatewayHandler pingHandler = gw.addEndPoint(gwOutputStream, gwInputStream,
                    new PingGatewayHandlerFactory(i, gwOutputStream, new PingMessageHandler(i, pingLatch)));

                gatewayHandlers.add(pingHandler);
            }

            helios.start();

            s2gAssociationLatch.await();
            g2sAssociationLatch.await();

            gatewayHandlers.forEach(PingGatewayHandler::sendPing);

            pingLatch.await();
        }

        assertTrue(true);
    }

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
            helios.addService((outputBuffers) -> null, null).addEndPoint(helios.newStream(OUTPUT_CHANNEL, 0));

        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenServiceResponseStreamIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addService((outputBuffers) -> null, helios.newStream(INPUT_CHANNEL, 0)).addEndPoint(null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenServiceFactoryIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addService(null, helios.newStream(INPUT_CHANNEL, 0));
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayRequestStreamIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addGateway().addEndPoint(null, helios.newStream(INPUT_CHANNEL, 0), (outputBuffers) -> null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayResponseStreamIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addGateway().addEndPoint(helios.newStream(OUTPUT_CHANNEL, 0), null, (outputBuffers) -> null);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayFactoryIsNull()
    {
        try(final Helios helios = new Helios())
        {
            helios.addGateway().addEndPoint(helios.newStream(OUTPUT_CHANNEL, 0), helios.newStream(INPUT_CHANNEL, 0), null);
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
        private final DataMessage incomingDataMessage = new DataMessage();

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
                final DataDecoder dataDecoder = incomingDataMessage.wrap(buffer, index, length);
                final ComponentDecoder componentDecoder = dataDecoder.mmbHeader().component();

                final ComponentType componentType = componentDecoder.componentType();
                assertTrue(componentType == ComponentType.Gateway);

                final int gatewayId = componentDecoder.componentId();

                //final int gatewayId = buffer.getInt(index); // FIXME: temporary workaround, use HGS protocol header

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
        //private final UnsafeBuffer echoBuffer;
        private final DataMessage outgoingDataMessage = new DataMessage();
        private final DataMessage incomingDataMessage = new DataMessage();

        PingGatewayHandler(final int gatewayId, final RingBufferPool ringBufferPool, final AeronStream gwOutputStream,
            final MessageHandler delegate)
        {
            this.gatewayId = gatewayId;
            this.ringBufferPool = ringBufferPool;
            this.gwOutputStream = gwOutputStream;
            this.delegate = delegate;
            this.idleStrategy = new BusySpinIdleStrategy();
            //this.echoBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));

            outgoingDataMessage.allocate(ComponentType.Gateway, (short)gatewayId, MESSAGE_LENGTH);
        }

        void sendPing()
        {
            final RingBuffer outputBuffer = ringBufferPool.getOutputRingBuffer(gwOutputStream); // FIXME: refactoring to avoid this API

            //echoBuffer.putInt(0, gatewayId); // FIXME: temporary workaround, use HGS protocol header
            final MutableDirectBuffer dataBuffer = outgoingDataMessage.dataBuffer();
            final int dataBufferOffset = outgoingDataMessage.dataBufferOffset();

            dataBuffer.putInt(dataBufferOffset, gatewayId);

            while (!outputBuffer.write(MessageTypes.APPLICATION_MSG_ID, dataBuffer, 0, dataBuffer.capacity()))
            {
                idleStrategy.idle(0);
            }
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            if (msgTypeId == MessageTypes.APPLICATION_MSG_ID)
            {
                incomingDataMessage.wrap(buffer, index, length);

                final MutableDirectBuffer dataBuffer = incomingDataMessage.dataBuffer();
                final int dataBufferOffset = incomingDataMessage.dataBufferOffset();
                final int dataBufferLength = incomingDataMessage.dataBufferLength();

                //
                delegate.onMessage(msgTypeId, dataBuffer, dataBufferOffset, dataBufferLength);
            }
            /*else
            {
                // ADMINISTRATIVE message type, ignore
            }*/

            //delegate.onMessage(msgTypeId, buffer, index, length);
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
