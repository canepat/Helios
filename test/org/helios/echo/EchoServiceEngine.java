package org.helios.echo;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import org.helios.mmb.Helios;
import org.helios.mmb.InputGear;
import org.helios.mmb.OutputGear;
import org.helios.util.ShutdownHelper;

import java.util.concurrent.CountDownLatch;

public class EchoServiceEngine
{
    public static void main(String[] args) throws Exception
    {
        System.out.print("Reading configuration properties...");

        final int inputBufferSize = Integer.getInteger("helios.core.engine.input_buffer_size", 512 * 1024);
        final int outputBufferSize = Integer.getInteger("helios.core.engine.output_buffer_size", 512 * 1024);
        final String inputChannel = System.getProperty("helios.mmb.input_channel", "udp://localhost:40123");
        final int inputStreamId = Integer.getInteger("helios.mmb.input_stream_id", 10);
        final String outputChannel = System.getProperty("helios.mmb.output_channel", "udp://localhost:40124");
        final int outputStreamId = Integer.getInteger("helios.mmb.output_stream_id", 10);

        System.out.print("done\nStarting Helios engine...");

        final CountDownLatch runningLatch = new CountDownLatch(1);

        try(final Helios helios = new Helios())
        {
            System.out.print("done\nCreating Helios I/O gears...");

            final WaitStrategy outputStrategy = new BusySpinWaitStrategy();
            final OutputGear outputGear = helios.addOutputGear(outputBufferSize, outputStrategy, outputChannel, outputStreamId);

            final WaitStrategy inputStrategy = new BusySpinWaitStrategy();
            final InputGear inputGear = helios.addInputGear(inputBufferSize, inputStrategy, inputChannel, inputStreamId);

            helios.addServiceHandler(new EchoServiceHandlerFactory(), inputGear, outputGear);

            System.out.println("done\nEchoServiceEngine is now running.");
            System.out.println(helios);

            ShutdownHelper.register(runningLatch::countDown);

            outputGear.start();
            inputGear.start();

            runningLatch.await();

            outputGear.stop();
            inputGear.stop();

            System.out.println("EchoServiceEngine is now terminated.");
        }
    }
}
