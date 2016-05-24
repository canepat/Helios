package org.helios.core.service;

import org.helios.infra.RateReport;

import java.io.PrintStream;

public final class ServiceReport implements RateReport
{
    private final GatewayRequestProcessor requestProcessor;
    private final GatewayResponseProcessor responseProcessor;

    private long lastTimeStamp;
    private long lastSuccessfulReads;
    private long lastSuccessfulWrites;

    public ServiceReport(final GatewayRequestProcessor requestProcessor, final GatewayResponseProcessor responseProcessor)
    {
        this.requestProcessor = requestProcessor;
        this.responseProcessor = responseProcessor;

        lastTimeStamp = System.currentTimeMillis();
        lastSuccessfulReads = requestProcessor.successfulReads();
        lastSuccessfulWrites = responseProcessor.handler().successfulWrites();
    }

    @Override
    public void print(final PrintStream stream)
    {
        final long newTimeStamp = System.currentTimeMillis();
        long newSuccessfulReads = requestProcessor.successfulReads();
        long newSuccessfulWrites = responseProcessor.handler().successfulWrites();

        final long duration = newTimeStamp - lastTimeStamp;
        final long successfulReadsDelta = newSuccessfulReads - lastSuccessfulReads;
        final long successfulWritesDelta = newSuccessfulWrites - lastSuccessfulWrites;

        stream.format("ServiceReport: T %dms IN %,d OK polls - OUT %,d OK offers\n",
            duration, successfulReadsDelta, successfulWritesDelta);

        lastTimeStamp = newTimeStamp;
        lastSuccessfulReads = newSuccessfulReads;
        lastSuccessfulWrites = newSuccessfulWrites;
    }
}
