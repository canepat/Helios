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
        lastSuccessfulWrites = requestProcessor.handler().successfulWrites();
        lastBytesWritten = requestProcessor.handler().bytesWritten();
        lastSuccessfulReads = responseProcessor.successfulReads();
    }

    @Override
    public void print(final PrintStream stream)
    {
        final long newTimeStamp = System.currentTimeMillis();
        long newSuccessfulWrites = requestProcessor.handler().successfulWrites();
        long newBytesWritten = requestProcessor.handler().bytesWritten();
        long newSuccessfulReads = responseProcessor.successfulReads();

        final long duration = newTimeStamp - lastTimeStamp;
        final long successfulWritesDelta = newSuccessfulWrites - lastSuccessfulWrites;
        final long bytesTransferred = newBytesWritten - lastBytesWritten;
        final long successfulReadsDelta = newSuccessfulReads - lastSuccessfulReads;

        stream.format("GatewayReport: T %dms OUT %,d messages - %,d bytes - IN %,d messages\n",
            duration, successfulWritesDelta, bytesTransferred, successfulReadsDelta);

        lastTimeStamp = newTimeStamp;
        lastSuccessfulWrites = newSuccessfulWrites;
        lastBytesWritten = newBytesWritten;
        lastSuccessfulReads = newSuccessfulReads;
    }
}
