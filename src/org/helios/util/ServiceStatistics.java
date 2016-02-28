package org.helios.util;

import java.util.concurrent.TimeUnit;

public class ServiceStatistics
{
    private static long serviceTime;
    private static long serviceOperations;

    public static void accumulate(final long elapsedTime)
    {
        serviceTime += elapsedTime;
        serviceOperations++;
    }

    public static String getResult()
    {
        return String.format("%d ops, %d ms, rate %.02g ops/s",
            serviceOperations,
            TimeUnit.NANOSECONDS.toMillis(serviceTime),
            ((double) serviceOperations / (double) serviceTime) * 1_000_000_000);
    }
}
