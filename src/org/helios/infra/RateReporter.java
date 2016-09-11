package org.helios.infra;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public final class RateReporter implements Processor
{
    private final List<RateReport> reportList;
    private final AtomicBoolean running;
    private final Thread reporterThread;

    public RateReporter()
    {
        reportList = new ArrayList<>();

        running = new AtomicBoolean(false);
        reporterThread = new Thread(this, "rateReporter");
    }

    public void add(final RateReport report)
    {
        reportList.add(report);
    }

    @Override
    public void start()
    {
        running.set(true);
        reporterThread.start();
    }

    @Override
    public void run()
    {
        while (running.get())
        {
            LockSupport.parkNanos(1_000_000_000L); // TODO: configure

            reportList.forEach(report -> report.print(System.out)); // TODO: write to SHM, publish via Aeron, log
        }
    }

    @Override
    public void close() throws Exception
    {
        running.set(false);
        reporterThread.join();
    }
}
