package echo;

import org.HdrHistogram.Histogram;
import org.agrona.LangUtil;
import org.agrona.console.ContinueBarrier;
import org.helios.AeronStream;
import org.helios.Helios;
import org.helios.HeliosContext;
import org.helios.gateway.Gateway;
import org.helios.infra.RateReport;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.System.nanoTime;

public class EchoGateway
{
    private static final String INPUT_CHANNEL = EchoConfiguration.SERVICE_OUTPUT_CHANNEL;
    private static final int INPUT_STREAM_ID = EchoConfiguration.SERVICE_OUTPUT_STREAM_ID;
    private static final String OUTPUT_CHANNEL = EchoConfiguration.SERVICE_INPUT_CHANNEL;
    private static final int OUTPUT_STREAM_ID = EchoConfiguration.SERVICE_INPUT_STREAM_ID;

    private static final int WARMUP_NUMBER_OF_MESSAGES = EchoConfiguration.WARMUP_NUMBER_OF_MESSAGES;
    private static final int WARMUP_NUMBER_OF_ITERATIONS = EchoConfiguration.WARMUP_NUMBER_OF_ITERATIONS;
    private static final int NUMBER_OF_MESSAGES = EchoConfiguration.NUMBER_OF_MESSAGES;
    private static final int NUMBER_OF_ITERATIONS = EchoConfiguration.NUMBER_OF_ITERATIONS;

    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
    private static final CountDownLatch ASSOCIATION_LATCH = new CountDownLatch(1);

    public static void main(String[] args) throws Exception
    {
        System.out.print("Starting Helios service...");

        final HeliosContext context = new HeliosContext();

        try(final Helios helios = new Helios(context))
        {
            helios.errorHandler(EchoGateway::serviceError);

            System.out.print("done\nCreating Helios gateway...");

            final AeronStream inputStream = helios.newStream(INPUT_CHANNEL, INPUT_STREAM_ID);
            final AeronStream outputStream = helios.newStream(OUTPUT_CHANNEL, OUTPUT_STREAM_ID);
            final Gateway<EchoGatewayHandler> gw = helios.addGateway(EchoGatewayHandler::new,
                EchoGateway::serviceAssociationEstablished, EchoGateway::serviceAssociationBroken,
                outputStream, inputStream);

            helios.start();

            System.out.print("Waiting for association with EchoService...");

            ASSOCIATION_LATCH.await();

            System.out.println("done\nEchoGateway is now running.");

            runTest(gw);

            System.out.println("EchoGateway is now terminated.");
        }
    }

    static void runTest(final Gateway<EchoGatewayHandler> gw)
    {
        final EchoGatewayHandler proxy = gw.handler();
        final List<RateReport> reportList = gw.reportList();

        System.out.println("Warming up... " + WARMUP_NUMBER_OF_ITERATIONS + " iterations of " + WARMUP_NUMBER_OF_MESSAGES + " messages");

        for (int i = 0; i < WARMUP_NUMBER_OF_ITERATIONS; i++)
        {
            runIterations(proxy, WARMUP_NUMBER_OF_MESSAGES);
        }

        final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

        int iteration = 0;
        do
        {
            iteration++;

            HISTOGRAM.reset();

            System.out.println("Echoing " + NUMBER_OF_MESSAGES + " messages");

            final long elapsedTime = runIterations(proxy, NUMBER_OF_MESSAGES);

            System.out.println(
                String.format("%d iteration, %d ops, %d ns, %d ms, rate %.02g ops/s",
                    iteration,
                    NUMBER_OF_MESSAGES,
                    elapsedTime,
                    TimeUnit.NANOSECONDS.toMillis(elapsedTime),
                    ((double)NUMBER_OF_MESSAGES / (double)elapsedTime) * 1_000_000_000));

            reportList.forEach(report -> report.print(System.out));

            if (NUMBER_OF_ITERATIONS <= 0)
            {
                System.out.println("Histogram of RTT latencies in microseconds");
                HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);
            }
        }
        while ((NUMBER_OF_ITERATIONS > 0 && iteration < NUMBER_OF_ITERATIONS) || barrier.await());
    }

    private static long runIterations(final EchoGatewayHandler gatewayHandler, final int numMessages)
    {
        final long start = nanoTime();

        for (int i = 0; i < numMessages; i++)
        {
            try
            {
                //final long echoTimestampSent = System.nanoTime();

                gatewayHandler.sendEcho(i);

                /*long echoTimestampReceived;
                do
                {
                    echoTimestampReceived = gatewayHandler.getTimestamp();
                }
                while (echoTimestampReceived != echoTimestampSent);

                final long echoRttNs = System.nanoTime() - echoTimestampReceived;

                HISTOGRAM.recordValue(echoRttNs);*/
            }
            catch (Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        final long end = nanoTime();

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
