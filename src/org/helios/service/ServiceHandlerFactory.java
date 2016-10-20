package org.helios.service;

import org.helios.util.RingBufferPool;

public interface ServiceHandlerFactory<T extends ServiceHandler>
{
    T createServiceHandler(final RingBufferPool ringBufferPool);
}
