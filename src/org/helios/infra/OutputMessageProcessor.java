package org.helios.infra;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;

public abstract class OutputMessageProcessor extends RingBufferProcessor<OutputMessageHandler>
{
    public OutputMessageProcessor(final RingBuffer outputRingBuffer, final AeronStream outputStream,
        final IdleStrategy idleStrategy, final String threadName)
    {
        this(outputRingBuffer, new OutputMessageHandler(outputStream, idleStrategy), idleStrategy, threadName);
    }

    public OutputMessageProcessor(final RingBuffer outputRingBuffer, final OutputMessageHandler outputHandler,
        final IdleStrategy idleStrategy, final String threadName)
    {
        super(outputRingBuffer, outputHandler, idleStrategy, threadName);
    }
}
