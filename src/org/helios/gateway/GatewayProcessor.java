package org.helios.gateway;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.RingBufferProcessor;

public final class GatewayProcessor<T extends GatewayHandler> extends RingBufferProcessor<T>
{
    public GatewayProcessor(final RingBuffer inputRingBuffer, final T gatewayHandler)
    {
        super(inputRingBuffer, gatewayHandler, new BusySpinIdleStrategy(), "gwProcessor");
    }
}
