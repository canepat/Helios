package org.helios.echo;

import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.core.service.ServiceHandlerFactory;

class EchoServiceHandlerFactory implements ServiceHandlerFactory<EchoServiceHandler>
{
    @Override
    public EchoServiceHandler createServiceHandler(final RingBuffer... outputBuffers)
    {
        return new EchoServiceHandler(outputBuffers[0]);
    }
}
