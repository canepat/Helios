package org.helios.gateway;

import org.agrona.concurrent.ringbuffer.RingBuffer;

public interface GatewayHandlerFactory<T extends GatewayHandler>
{
    T createGatewayHandler(final RingBuffer ringBuffer);
}
