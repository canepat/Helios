package echo;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.helios.AeronStream;
import org.helios.Helios;
import org.helios.HeliosContext;
import org.helios.HeliosDriver;
import org.helios.gateway.Gateway;
import org.helios.infra.ConsoleReporter;
import org.helios.service.Service;

import java.util.concurrent.CountDownLatch;

public class EchoEmbeddedIpc
{
    private static final int SERVICE_INPUT_STREAM_ID = EchoConfiguration.SERVICE_INPUT_STREAM_ID;
    private static final int SERVICE_OUTPUT_STREAM_ID = EchoConfiguration.SERVICE_OUTPUT_STREAM_ID;

    private static final CountDownLatch GW_ASSOCIATION_LATCH = new CountDownLatch(1);
    private static final CountDownLatch SVC_ASSOCIATION_LATCH = new CountDownLatch(1);

    public static void main(String[] args) throws Exception
    {
        System.out.print("Starting Helios...");

        final HeliosContext context = new HeliosContext()
            //.setJournalEnabled(true)
            .setReadIdleStrategy(new BusySpinIdleStrategy())
            .setWriteIdleStrategy(new BusySpinIdleStrategy())
            .setSubscriberIdleStrategy(new BusySpinIdleStrategy())
            .setPublisherIdleStrategy(new BusySpinIdleStrategy());

        final HeliosDriver driver = new HeliosDriver(context);

        try(final Helios helios = new Helios(context, driver))
        {
            helios.errorHandler(EchoEmbeddedIpc::serviceError);

            System.out.print("done\nCreating Helios service...");

            final AeronStream ipcInputStream = helios.newIpcStream(SERVICE_INPUT_STREAM_ID);
            final AeronStream ipcOutputStream = helios.newIpcStream(SERVICE_OUTPUT_STREAM_ID);
            final Service<EchoServiceHandler> svc = helios.addService(EchoServiceHandler::new,
                EchoEmbeddedIpc::associationWithGatewayEstablished, EchoEmbeddedIpc::associationWithGatewayBroken,
                ipcInputStream, ipcOutputStream);

            System.out.print("done\nCreating Helios gateway...");

            final Gateway<EchoGatewayHandler> gw = helios.addGateway(
                EchoEmbeddedIpc::associationWithServiceEstablished, EchoEmbeddedIpc::associationWithServiceBroken);
            final EchoGatewayHandler gwHandler = gw.addEndPoint(ipcInputStream, ipcOutputStream, EchoGatewayHandler::new);

            final ConsoleReporter reporter = new ConsoleReporter();

            System.out.println("done\nEchoEmbedded is now running.");

            helios.start();

            System.out.print("Waiting for Gateway to see association with Service...");

            GW_ASSOCIATION_LATCH.await();

            System.out.print("done\nWaiting for Service to see association with Gateway...");

            SVC_ASSOCIATION_LATCH.await();

            System.out.println("done");

            EchoGateway.runTest(gwHandler, () -> reporter.onReport(gw.report()));

            reporter.onReport(gw.report());
            reporter.onReport(svc.report());
        }

        System.out.println("EchoEmbeddedIpc is now terminated.");
        System.exit(0);
    }

    private static void serviceError(final Throwable th)
    {
        th.printStackTrace();
    }

    private static void associationWithServiceEstablished()
    {
        GW_ASSOCIATION_LATCH.countDown();
    }

    private static void associationWithServiceBroken()
    {
        System.out.println("Association with Service broken.");
    }

    private static void associationWithGatewayEstablished()
    {
        SVC_ASSOCIATION_LATCH.countDown();
    }

    private static void associationWithGatewayBroken()
    {
        System.out.println("Association with Gateway broken.");
    }
}
