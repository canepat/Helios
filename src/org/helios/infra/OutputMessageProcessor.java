package org.helios.infra;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;

public class OutputMessageProcessor extends RingBufferProcessor<OutputMessageHandler>
{
    public OutputMessageProcessor(final RingBuffer outputRingBuffer, final AeronStream outputStream,
        final IdleStrategy idleStrategy, final String threadName)
    {
        super(outputRingBuffer, new OutputMessageHandler(outputStream, idleStrategy), idleStrategy, threadName);
    }
}
