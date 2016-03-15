package org.helios.echo;

import org.helios.Helios;
import org.helios.HeliosContext;
import org.helios.mmb.InputGear;
import org.helios.mmb.OutputGear;
import org.helios.util.ShutdownHelper;

import java.util.concurrent.CountDownLatch;

public class EchoServiceEngine
{
    private static final int INPUT_RING_SIZE = EchoConfiguration.SERVICE_INPUT_RING_SIZE;
    private static final int OUTPUT_RING_SIZE = EchoConfiguration.SERVICE_OUTPUT_RING_SIZE;
    private static final String INPUT_CHANNEL = EchoConfiguration.SERVICE_INPUT_CHANNEL;
    private static final int INPUT_STREAM_ID = EchoConfiguration.SERVICE_INPUT_STREAM_ID;
    private static final String OUTPUT_CHANNEL = EchoConfiguration.SERVICE_OUTPUT_CHANNEL;
    private static final int OUTPUT_STREAM_ID = EchoConfiguration.SERVICE_OUTPUT_STREAM_ID;

    public static void main(String[] args) throws Exception
    {
        System.out.print("Starting Helios engine...");

        final HeliosContext context = new HeliosContext();
        context.setJournalEnabled(true);

        try(final Helios helios = new Helios(context))
        {
            System.out.print("done\nCreating Helios I/O gears...");

            final OutputGear outputGear = helios.addOutputGear(OUTPUT_RING_SIZE, OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            final InputGear inputGear = helios.addInputGear(INPUT_RING_SIZE, INPUT_CHANNEL, INPUT_STREAM_ID);

            helios.addServiceHandler(new EchoServiceHandlerFactory(), inputGear, outputGear);

            final CountDownLatch runningLatch = new CountDownLatch(1);
            ShutdownHelper.register(runningLatch::countDown);

            System.out.println("done\nEchoServiceEngine is now running.");

            outputGear.start();
            inputGear.start();

            runningLatch.await();

            outputGear.stop();
            inputGear.stop();

            System.out.println("EchoServiceEngine is now terminated.");
        }
    }
}
