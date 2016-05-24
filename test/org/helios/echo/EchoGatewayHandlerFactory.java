package org.helios.echo;

import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.gateway.GatewayHandlerFactory;

class EchoGatewayHandlerFactory implements GatewayHandlerFactory<EchoGatewayHandler>
{
    @Override
    public EchoGatewayHandler createGatewayHandler(final RingBuffer... outputBuffers)
    {
        return new EchoGatewayHandler(outputBuffers[0]);
    }
}
