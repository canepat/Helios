package org.helios.core.service;

import org.helios.infra.InputMessageProcessor;
import org.helios.infra.OutputMessageProcessor;
import org.helios.infra.RateReport;

import java.io.PrintStream;

public final class ServiceReport implements RateReport
{
    private final InputMessageProcessor requestProcessor;
    private final OutputMessageProcessor responseProcessor;

    private long lastTimeStamp;
    private long lastSuccessfulReads;
    private long lastSuccessfulWrites;
    private long lastBytesWritten;

    public ServiceReport(final InputMessageProcessor requestProcessor, final OutputMessageProcessor responseProcessor)
    {
        this.requestProcessor = requestProcessor;
        this.responseProcessor = responseProcessor;

        lastTimeStamp = System.currentTimeMillis();
        lastSuccessfulReads = requestProcessor.successfulReads();
        lastSuccessfulWrites = responseProcessor.handler().successfulWrites();
        lastBytesWritten = responseProcessor.handler().bytesWritten();
    }

    @Override
    public void print(final PrintStream stream)
    {
        final long newTimeStamp = System.currentTimeMillis();
        long newSuccessfulReads = requestProcessor.successfulReads();
        long newSuccessfulWrites = responseProcessor.handler().successfulWrites();
        long newBytesWritten = responseProcessor.handler().bytesWritten();

        final long duration = newTimeStamp - lastTimeStamp;
        final long successfulReadsDelta = newSuccessfulReads - lastSuccessfulReads;
        final long successfulWritesDelta = newSuccessfulWrites - lastSuccessfulWrites;
        final long bytesTransferred = newBytesWritten - lastBytesWritten;

        stream.format("ServiceReport: T %dms IN %,d messages - OUT %,d messages - %,d bytes\n",
            duration, successfulReadsDelta, successfulWritesDelta, bytesTransferred);

        lastTimeStamp = newTimeStamp;
        lastSuccessfulReads = newSuccessfulReads;
        lastSuccessfulWrites = newSuccessfulWrites;
        lastBytesWritten = newBytesWritten;
    }
}
