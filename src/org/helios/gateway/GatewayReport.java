package org.helios.gateway;

import org.helios.infra.InputMessageProcessor;
import org.helios.infra.OutputMessageProcessor;
import org.helios.infra.RateReport;

import java.io.PrintStream;

public final class GatewayReport implements RateReport
{
    private final OutputMessageProcessor requestProcessor;
    private final InputMessageProcessor responseProcessor;

    private long lastTimeStamp;
    private long lastSuccessfulWrites;
    private long lastBytesWritten;
    private long lastSuccessfulReads;

    public GatewayReport(final OutputMessageProcessor requestProcessor, final InputMessageProcessor responseProcessor)
    {
        this.requestProcessor = requestProcessor;
        this.responseProcessor = responseProcessor;

        lastTimeStamp = System.currentTimeMillis();
    }

    @Override
    public void print(final PrintStream stream)
    {
        final long timeStamp = System.currentTimeMillis();
        long successfulWrites = requestProcessor.handler().successfulWrites();
        long bytesWritten = requestProcessor.handler().bytesWritten();
        long successfulReads = responseProcessor.successfulReads();
        long failedReads = responseProcessor.failedReads();

        final long duration = timeStamp - lastTimeStamp;
        final long successfulWritesDelta = successfulWrites - lastSuccessfulWrites;
        final long bytesTransferred = bytesWritten - lastBytesWritten;
        final long successfulReadsDelta = successfulReads - lastSuccessfulReads;

        final double failureRatio = failedReads / (double)(successfulReads + failedReads);

        stream.format("GatewayReport: T %dms OUT %,d messages - %,d bytes - IN %,d messages [read failure ratio: %f]\n",
            duration, successfulWritesDelta, bytesTransferred, successfulReadsDelta, failureRatio);

        lastTimeStamp = timeStamp;
        lastSuccessfulWrites = successfulWrites;
        lastBytesWritten = bytesWritten;
        lastSuccessfulReads = successfulReads;
    }
}
