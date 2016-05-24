package org.helios.gateway;

import org.helios.infra.RateReport;

import java.io.PrintStream;

public final class GatewayReport implements RateReport
{
    private final ServiceRequestProcessor requestProcessor;
    private final ServiceResponseProcessor responseProcessor;

    private long lastTimeStamp;
    private long lastMsgRequested;
    private long lastBytesRequested;
    private long lastSubscriptionSuccessfulPolls;

    public GatewayReport(final ServiceRequestProcessor requestProcessor, final ServiceResponseProcessor responseProcessor)
    {
        this.requestProcessor = requestProcessor;
        this.responseProcessor = responseProcessor;

        lastTimeStamp = System.currentTimeMillis();
        lastMsgRequested = requestProcessor.msgRequested();
        lastBytesRequested = requestProcessor.bytesRequested();
        lastSubscriptionSuccessfulPolls = responseProcessor.successfulReads();
    }

    @Override
    public void print(final PrintStream stream)
    {
        final long newTimeStamp = System.currentTimeMillis();
        long newMsgRequested = requestProcessor.msgRequested();
        long newBytesRequested = requestProcessor.bytesRequested();
        long newSubscriptionSuccessfulPolls = responseProcessor.successfulReads();

        final long duration = newTimeStamp - lastTimeStamp;
        final long msgTransferred = newMsgRequested - lastMsgRequested;
        final long bytesTransferred = newBytesRequested - lastBytesRequested;
        final long successfulPollDelta = newSubscriptionSuccessfulPolls - lastSubscriptionSuccessfulPolls;

        stream.format("GatewayReport: T %dms OUT %,d messages - %,d bytes - IN %,d OK polls\n",
            duration, msgTransferred, bytesTransferred, successfulPollDelta);

        lastTimeStamp = newTimeStamp;
        lastMsgRequested = newMsgRequested;
        lastBytesRequested = newBytesRequested;
        lastSubscriptionSuccessfulPolls = newSubscriptionSuccessfulPolls;
    }
}
