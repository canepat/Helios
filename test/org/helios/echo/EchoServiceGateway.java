package org.helios.echo;

import org.HdrHistogram.Histogram;
import org.helios.Helios;
import org.helios.HeliosContext;
import org.helios.util.DirectBufferAllocator;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.console.ContinueBarrier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class EchoServiceGateway
{
    public static final String INPUT_CHANNEL = EchoConfiguration.SERVICE_OUTPUT_CHANNEL;
    public static final int INPUT_STREAM_ID = EchoConfiguration.SERVICE_OUTPUT_STREAM_ID;
    public static final String OUTPUT_CHANNEL = EchoConfiguration.SERVICE_INPUT_CHANNEL;
    public static final int OUTPUT_STREAM_ID = EchoConfiguration.SERVICE_INPUT_STREAM_ID;

    private static final int WARMUP_NUMBER_OF_MESSAGES = EchoConfiguration.WARMUP_NUMBER_OF_MESSAGES;
    private static final int WARMUP_NUMBER_OF_ITERATIONS = EchoConfiguration.WARMUP_NUMBER_OF_ITERATIONS;
    private static final int NUMBER_OF_MESSAGES = EchoConfiguration.NUMBER_OF_MESSAGES;
    private static final int MESSAGE_LENGTH = EchoConfiguration.MESSAGE_LENGTH;

    private static final UnsafeBuffer ATOMIC_BUFFER = new UnsafeBuffer(DirectBufferAllocator.allocateDirect(MESSAGE_LENGTH));
    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
    private static final CountDownLatch ASSOCIATION_LATCH = new CountDownLatch(1);

    public static void main(String[] args) throws Exception
    {
        System.out.print("Starting Helios engine...");

        final HeliosContext context = new HeliosContext();

        try(final Helios helios = new Helios(context))
        {
            helios.errorHandler(EchoServiceGateway::serviceError)
                .availableAssociationHandler(EchoServiceGateway::serviceAssociationEstablished)
                .unavailableAssociationHandler(EchoServiceGateway::serviceAssociationBroken);

            System.out.print("done\nCreating Helios I/O gears...");

            final EchoServiceProxy proxy = helios.addServiceProxy(new EchoServiceProxyFactory(), INPUT_CHANNEL, INPUT_STREAM_ID, OUTPUT_CHANNEL, OUTPUT_STREAM_ID);

            System.out.println("done\nEchoServiceGateway is now running.");

            System.out.print("Waiting for association with EchoServiceEngine...");

            ASSOCIATION_LATCH.await();

            System.out.println("done\nEchoServiceGateway is now running.");

            runTest(proxy);

            System.out.println("EchoServiceGateway is now terminated.");
        }
    }

    private static void runTest(final EchoServiceProxy proxy)
    {
        System.out.println("Warming up... " + WARMUP_NUMBER_OF_ITERATIONS + " iterations of " + WARMUP_NUMBER_OF_MESSAGES + " messages");

        for (int i = 0; i < WARMUP_NUMBER_OF_ITERATIONS; i++)
        {
            runIterations(proxy, WARMUP_NUMBER_OF_MESSAGES);
        }

        final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

        do
        {
            HISTOGRAM.reset();

            System.out.println("Echoing " + NUMBER_OF_MESSAGES + " messages");

            final long elapsedTime = runIterations(proxy, NUMBER_OF_MESSAGES);

            System.out.println(
                String.format("%d ops, %d ns, %d ms, rate %.02g ops/s",
                    NUMBER_OF_MESSAGES,
                    elapsedTime,
                    TimeUnit.NANOSECONDS.toMillis(elapsedTime),
                    ((double)NUMBER_OF_MESSAGES / (double)elapsedTime) * 1_000_000_000));

            System.out.println("Histogram of RTT latencies in microseconds");
            HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);
        }
        while (barrier.await());
    }

    private static long runIterations(final EchoServiceProxy proxy, final int numMessages)
    {
        final long start = System.nanoTime();

        for (int i = 0; i < numMessages; i++)
        {
            try
            {
                do
                {
                    ATOMIC_BUFFER.putLong(0, System.nanoTime());
                }
                while (proxy.send(ATOMIC_BUFFER, MESSAGE_LENGTH) < 0L);

                while (proxy.receive() <= 0)
                {
                    proxy.idle(0);
                }

                final long echoTimestamp = proxy.getTimestamp();

                final long echoRttNs = System.nanoTime() - echoTimestamp;

                HISTOGRAM.recordValue(echoRttNs);
            }
            catch (Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        final long end = System.nanoTime();

        return (end - start);
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
