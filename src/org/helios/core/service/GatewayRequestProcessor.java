package org.helios.core.service;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;
import org.helios.infra.InputMessageHandler;
import org.helios.infra.InputMessageProcessor;

public final class GatewayRequestProcessor extends InputMessageProcessor
{
    private static final int FRAME_COUNT_LIMIT = Integer.getInteger("helios.service.poll.frame_count_limit", 10);

    public GatewayRequestProcessor(final RingBuffer inputRingBuffer, final AeronStream requestStream,
        final IdleStrategy writeIdleStrategy, final IdleStrategy pollIdleStrategy)
    {
        this(new InputMessageHandler(inputRingBuffer, writeIdleStrategy), requestStream, pollIdleStrategy);
    }

    public GatewayRequestProcessor(final InputMessageHandler handler, final AeronStream requestStream,
        final IdleStrategy pollIdleStrategy)
    {
        super(handler, requestStream, pollIdleStrategy, FRAME_COUNT_LIMIT, "gwRequestProcessor");
    }
}
