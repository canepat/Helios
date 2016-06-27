package echo;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.helios.Helios;
import org.helios.HeliosContext;
import org.helios.HeliosDriver;
import org.helios.core.service.Service;
import org.helios.gateway.Gateway;

import java.util.concurrent.CountDownLatch;

public class EchoEmbedded
{
    private static final int SERVICE_INPUT_STREAM_ID = EchoConfiguration.SERVICE_INPUT_STREAM_ID;
    private static final int SERVICE_OUTPUT_STREAM_ID = EchoConfiguration.SERVICE_OUTPUT_STREAM_ID;

    private static final CountDownLatch ASSOCIATION_LATCH = new CountDownLatch(1);

    public static void main(String[] args) throws Exception
    {
        System.out.print("Starting Helios service...");

        final HeliosContext context = new HeliosContext()
            //.setJournalEnabled(true)
            .setReadIdleStrategy(new BusySpinIdleStrategy())
            .setWriteIdleStrategy(new BusySpinIdleStrategy())
            .setSubscriberIdleStrategy(new BusySpinIdleStrategy())
            .setPublisherIdleStrategy(new BusySpinIdleStrategy());

        final HeliosDriver driver = new HeliosDriver(context);

        try(final Helios helios = new Helios(context, driver))
        {
            helios.errorHandler(EchoEmbedded::serviceError);

            System.out.print("done\nCreating Helios service and gateway...");

            helios.addEmbeddedService(
                SERVICE_INPUT_STREAM_ID, SERVICE_OUTPUT_STREAM_ID, new EchoServiceHandlerFactory());
            final Gateway<EchoGatewayHandler> gw = helios.addEmbeddedGateway(
                SERVICE_INPUT_STREAM_ID, SERVICE_OUTPUT_STREAM_ID, new EchoGatewayHandlerFactory());
            gw.availableAssociationHandler(EchoEmbedded::serviceAssociationEstablished);
            gw.unavailableAssociationHandler(EchoEmbedded::serviceAssociationBroken);

            System.out.println("done\nEchoEmbedded is now running.");

            helios.start();

            System.out.print("Waiting for association between Gateway and Service...");

            ASSOCIATION_LATCH.await();

            System.out.println("done");

            EchoGateway.runTest(gw);
        }

        System.out.println("EchoEmbedded is now terminated.");
        System.exit(0);
    }

    private static void serviceError(final Throwable th)
    {
        th.printStackTrace();
    }

    private static void serviceAssociationEstablished()
    {
        ASSOCIATION_LATCH.countDown();
    }

    private static void serviceAssociationBroken()
    {
    }
}
