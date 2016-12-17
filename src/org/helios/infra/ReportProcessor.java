package org.helios.infra;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;

public final class ReportProcessor implements Processor
{
    private final long reportInterval;
    private final Reporter reportingFunc;
    private final List<Report> reportList;
    private volatile boolean running;
    private Thread reporterThread;

    public ReportProcessor(final long reportInterval, final Reporter reportingFunc)
    {
        Objects.requireNonNull(reportingFunc, "reportingFunc");

        this.reportInterval = reportInterval;
        this.reportingFunc = reportingFunc;
        reportList = new ArrayList<>();
    }

    public void add(final Report report)
    {
        Objects.requireNonNull(report, "report");

        reportList.add(report);
    }

    @Override
    public String name()
    {
        return reporterThread.getName();
    }

    @Override
    public void start()
    {
        running = true;
        reporterThread = new Thread(this, "reportProcessor");
        reporterThread.start();
    }

    @Override
    public void run()
    {
        do
        {
            LockSupport.parkNanos(reportInterval);

            reportList.forEach(reportingFunc::onReport); // TODO: write to SHM, publish via Aeron, log
        }
        while (running);
    }

    @Override
    public void close() throws Exception
    {
        running = false;
        reporterThread.join();
    }
}
