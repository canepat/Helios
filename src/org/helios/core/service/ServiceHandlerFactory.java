package org.helios.core.service;

import org.agrona.concurrent.ringbuffer.RingBuffer;

public interface ServiceHandlerFactory<T extends ServiceHandler>
{
    T createServiceHandler(final RingBuffer... outputBuffers);
}
