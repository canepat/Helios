package org.helios;

import org.agrona.MutableDirectBuffer;
import org.helios.core.service.ServiceHandler;
import org.helios.gateway.GatewayHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

public class HeliosTest
{
    private static final int SVC_INPUT_STREAM_ID = 10;
    private static final int SVC_OUTPUT_STREAM_ID = 11;
    private static final int NUM_GATEWAYS = 5;

    private static Helios helios;

    @BeforeClass
    public static void setUpBeforeClass()
    {
        helios = new Helios(new HeliosContext());
    }

    @Test
    public void shouldAssociateOneGatewayWithOneService() throws Exception
    {
        try(final Helios helios = new Helios(new HeliosContext()))
        {
            final CountDownLatch associationLatch = new CountDownLatch(1);

            helios.addEmbeddedService(SVC_INPUT_STREAM_ID, SVC_OUTPUT_STREAM_ID,
                (outputBuffers) -> new NullServiceHandler());
            helios.addEmbeddedGateway(SVC_INPUT_STREAM_ID, SVC_OUTPUT_STREAM_ID,
                (outputBuffers) -> new NullGatewayHandler())
                .availableAssociationHandler(associationLatch::countDown);

            helios.start();

            associationLatch.await();

            assertTrue(true);
        }
    }

    @Test
    public void shouldAssociateMultipleGatewaysWithOneService() throws Exception
    {
        try(final Helios helios = new Helios(new HeliosContext()))
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

            assertTrue(true);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenDriverIsNull()
    {
        new Helios(new HeliosContext(), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenContextIsNull()
    {
        new Helios(null, new HeliosDriver(new HeliosContext()));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenOnlyContextIsNull()
    {
        new Helios(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenErrorHandlerIsNull()
    {
        helios.errorHandler(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenStreamChannelIsNull()
    {
        helios.newStream(null, 0);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenEmbeddedServiceFactoryIsNull()
    {
        helios.addEmbeddedService(0, 0, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenServiceRequestStreamIsNull()
    {
        helios.addService(null, helios.newStream("", 0), (outputBuffers) -> null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenServiceResponseStreamIsNull()
    {
        helios.addService(helios.newStream("", 0), null, (outputBuffers) -> null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenServiceFactoryIsNull()
    {
        helios.addService(helios.newStream("", 0), helios.newStream("", 0), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenEmbeddedGatewayFactoryIsNull()
    {
        helios.addEmbeddedGateway(0, 0, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayRequestStreamIsNull()
    {
        helios.addGateway(null, helios.newStream("", 0), (outputBuffers) -> null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayResponseStreamIsNull()
    {
        helios.addGateway(helios.newStream("", 0), null, (outputBuffers) -> null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenGatewayFactoryIsNull()
    {
        helios.addGateway(helios.newStream("", 0), helios.newStream("", 0), null);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        helios.close();
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
}
