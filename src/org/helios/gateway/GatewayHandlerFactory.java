package org.helios.gateway;

import org.helios.util.RingBufferPool;

public interface GatewayHandlerFactory<T extends GatewayHandler>
{
    T createGatewayHandler(final RingBufferPool ringBufferPool);
}
