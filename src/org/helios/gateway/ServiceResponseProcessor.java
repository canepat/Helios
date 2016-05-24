package org.helios.gateway;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;
import org.helios.infra.InputMessageHandler;
import org.helios.infra.InputMessageProcessor;

public final class ServiceResponseProcessor extends InputMessageProcessor
{
    private static final int FRAME_COUNT_LIMIT = Integer.getInteger("helios.gateway.poll.frame_count_limit", 10);

    public ServiceResponseProcessor(final RingBuffer inputRingBuffer, final AeronStream rspStream)
    {
        this(new InputMessageHandler(inputRingBuffer, new BusySpinIdleStrategy()), rspStream);
    }

    public ServiceResponseProcessor(final InputMessageHandler handler, final AeronStream rspStream)
    {
        super(handler, rspStream, new BusySpinIdleStrategy(), FRAME_COUNT_LIMIT, "svcResponseProcessor");
    }
}
