package org.helios.echo;

import org.helios.AeronStream;
import org.helios.Helios;
import org.helios.HeliosContext;
import org.helios.util.ShutdownHelper;

import java.util.concurrent.CountDownLatch;

public class EchoServiceEngine
{
    private static final String INPUT_CHANNEL = EchoConfiguration.SERVICE_INPUT_CHANNEL;
    private static final int INPUT_STREAM_ID = EchoConfiguration.SERVICE_INPUT_STREAM_ID;
    private static final String OUTPUT_CHANNEL = EchoConfiguration.SERVICE_OUTPUT_CHANNEL;
    private static final int OUTPUT_STREAM_ID = EchoConfiguration.SERVICE_OUTPUT_STREAM_ID;

    public static void main(String[] args) throws Exception
    {
        System.out.print("Starting Helios service...");

        final HeliosContext context = new HeliosContext()
            .setJournalEnabled(true);

        try(final Helios helios = new Helios(context))
        {
            System.out.print("done\nCreating Helios service...");

            final AeronStream inputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream outputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            helios.addService(inputStream, outputStream, new EchoServiceHandlerFactory());

            final CountDownLatch runningLatch = new CountDownLatch(1);
            ShutdownHelper.register(runningLatch::countDown);

            System.out.println("done\nEchoServiceEngine is now running.");

            helios.start();

            runningLatch.await();
        }

        System.out.println("EchoServiceEngine is now terminated.");
    }
}