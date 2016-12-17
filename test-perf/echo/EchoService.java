package echo;

import org.helios.AeronStream;
import org.helios.Helios;
import org.helios.HeliosContext;
import org.helios.HeliosDriver;
import org.helios.infra.ConsoleReporter;
import org.helios.service.Service;
import org.helios.util.ShutdownHelper;

import java.util.concurrent.CountDownLatch;

public class EchoService
{
    private static final String INPUT_CHANNEL = EchoConfiguration.SERVICE_INPUT_CHANNEL;
    private static final int INPUT_STREAM_ID = EchoConfiguration.SERVICE_INPUT_STREAM_ID;
    private static final String OUTPUT_CHANNEL = EchoConfiguration.SERVICE_OUTPUT_CHANNEL;
    private static final int OUTPUT_STREAM_ID = EchoConfiguration.SERVICE_OUTPUT_STREAM_ID;

    public static void main(String[] args) throws Exception
    {
        System.out.print("Starting Helios...");

        final HeliosContext context = new HeliosContext()
            .setJournalEnabled(true);
        final HeliosDriver driver = new HeliosDriver(context, "./.aeronService");

        try(final Helios helios = new Helios(context, driver))
        {
            System.out.print("done\nCreating Helios service...");

            final AeronStream inputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream outputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            final Service<EchoServiceHandler> svc = helios.addService(
                EchoServiceHandler::new, inputStream, outputStream);

            final EchoServiceHandler echoServiceHandler = svc.handler();

            final CountDownLatch runningLatch = new CountDownLatch(1);
            ShutdownHelper.register(runningLatch::countDown);

            helios.start();

            System.out.println("done\nEchoService is now running");

            runningLatch.await();

            System.out.println("EchoService last snapshot: " + echoServiceHandler.lastSnapshotTimestamp());

            new ConsoleReporter().onReport(svc.report());
        }

        System.out.println("EchoService is now terminated");
    }
}
