package org.helios.echo;

import org.helios.Helios;
import org.helios.HeliosContext;
import org.helios.mmb.InputGear;
import org.helios.mmb.OutputGear;
import org.helios.util.ShutdownHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;

public class EchoServiceGateway implements Runnable
{
    public static void main(String[] args) throws Exception
    {
        System.out.print("Reading configuration properties...");

        final int inputBufferSize = getInteger("echo.gateway.input_buffer_size", 512 * 1024);
        final int outputBufferSize = getInteger("echo.gateway.output_buffer_size", 512 * 1024);
        final String inputChannel = getProperty("echo.gateway.input_channel", "udp://localhost:40124");
        final int inputStreamId = getInteger("echo.gateway.input_stream_id", 10);
        final String outputChannel = getProperty("echo.gateway.output_channel", "udp://localhost:40123");
        final int outputStreamId = getInteger("echo.gateway.output_stream_id", 10);

        System.out.print("done\nStarting Helios engine...");

        final HeliosContext context = new HeliosContext();

        final CountDownLatch runningLatch = new CountDownLatch(1);

        try(final Helios helios = new Helios(context))
        {
            System.out.print("done\nCreating Helios I/O gears...");

            final OutputGear outputGear = helios.addOutputGear(outputBufferSize, outputChannel, outputStreamId);
            final InputGear inputGear = helios.addInputGear(inputBufferSize, inputChannel, inputStreamId);

            helios.addServiceProxy(new EchoServiceProxyFactory(), inputGear, outputGear);

            System.out.println("done\nEchoServiceGateway is now running.");
            System.out.println(helios);

            ShutdownHelper.register(runningLatch::countDown);

            outputGear.start();
            inputGear.start();

            runningLatch.await();

            outputGear.stop();
            inputGear.stop();

            System.out.println("EchoServiceGateway is now terminated.");
        }
    }

    private final AtomicBoolean running;

    public EchoServiceGateway(final AtomicBoolean running)
    {
        this.running = running;
    }

    @Override
    public void run()
    {
        while (running.get())
        {

        }
    }
}
