package org.helios.service;

import org.helios.infra.InputMessageProcessor;
import org.helios.infra.OutputMessageProcessor;
import org.helios.infra.RateReport;

import java.io.PrintStream;
import java.util.Objects;

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
        Objects.requireNonNull(requestProcessor, "requestProcessor");
        Objects.requireNonNull(responseProcessor, "responseProcessor");

        this.requestProcessor = requestProcessor;
        this.responseProcessor = responseProcessor;

        lastTimeStamp = System.currentTimeMillis();
    }

    @Override
    public void print(final PrintStream stream)
    {
        final long timeStamp = System.currentTimeMillis();
        long successfulReads = requestProcessor.successfulReads();
        long successfulWrites = responseProcessor.handler().successfulWrites();
        long bytesWritten = responseProcessor.handler().bytesWritten();
        long failedReads = requestProcessor.failedReads();

        final long duration = timeStamp - lastTimeStamp;
        final long successfulReadsDelta = successfulReads - lastSuccessfulReads;
        final long successfulWritesDelta = successfulWrites - lastSuccessfulWrites;
        final long bytesTransferred = bytesWritten - lastBytesWritten;

        final double failureRatio = failedReads / (double)(successfulReads + failedReads);

        stream.format("ServiceReport: T %dms IN %,d messages - OUT %,d messages - %,d bytes [read failure ratio: %f]\n",
            duration, successfulReadsDelta, successfulWritesDelta, bytesTransferred, failureRatio);

        lastTimeStamp = timeStamp;
        lastSuccessfulReads = successfulReads;
        lastSuccessfulWrites = successfulWrites;
        lastBytesWritten = bytesWritten;
    }
}
