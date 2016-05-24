package org.helios.core.service;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;
import org.helios.infra.OutputMessageProcessor;

public final class GatewayResponseProcessor extends OutputMessageProcessor
{
    public GatewayResponseProcessor(final RingBuffer outputRingBuffer, final AeronStream rspStream)
    {
        super(outputRingBuffer, rspStream, new BusySpinIdleStrategy(), "gwResponseProcessor");
    }
}
