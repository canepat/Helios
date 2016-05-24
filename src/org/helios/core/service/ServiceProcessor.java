package org.helios.core.service;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.RingBufferProcessor;

public final class ServiceProcessor<T extends ServiceHandler> extends RingBufferProcessor<T>
{
    public ServiceProcessor(final RingBuffer serviceRingBuffer, final T serviceHandler)
    {
        super(serviceRingBuffer, serviceHandler, new BusySpinIdleStrategy(), "serviceProcessor");
    }
}
