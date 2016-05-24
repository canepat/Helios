package org.helios.echo;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.core.MessageTypes;
import org.helios.core.service.ServiceHandler;

public class EchoServiceHandler implements ServiceHandler
{
    private final RingBuffer outputBuffer;
    private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

    public EchoServiceHandler(final RingBuffer outputBuffer)
    {
        this.outputBuffer = outputBuffer;
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        /* ECHO SERVICE message processing: reply the received message itself */

        while (!outputBuffer.write(MessageTypes.APPLICATION_MSG_ID, buffer, index, length))
        {
            idleStrategy.idle(0);
        }
    }

    @Override
    public void close() throws Exception
    {
    }
}
