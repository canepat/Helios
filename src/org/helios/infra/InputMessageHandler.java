package org.helios.infra;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;

public class InputMessageHandler implements FragmentHandler, AutoCloseable
{
    private final RingBuffer inputRingBuffer;
    private final IdleStrategy idleStrategy;

    public InputMessageHandler(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy)
    {
        this.inputRingBuffer = inputRingBuffer;
        this.idleStrategy = idleStrategy;
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header)
    {
        while (!inputRingBuffer.write(MessageTypes.APPLICATION_MSG_ID, buffer, offset, length))
        {
            idleStrategy.idle(0);
        }
    }

    @Override
    public void close() throws Exception
    {
    }
}
