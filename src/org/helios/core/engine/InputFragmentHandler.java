package org.helios.core.engine;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;

public class InputFragmentHandler implements FragmentHandler
{
    private final Disruptor<InputBufferEvent> inputDisruptor;

    public InputFragmentHandler(final Disruptor<InputBufferEvent> inputDisruptor)
    {
        this.inputDisruptor = inputDisruptor;
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header)
    {
        final long requestTime = System.nanoTime();

        final RingBuffer<InputBufferEvent> ringBuffer = inputDisruptor.getRingBuffer();

        final long sequence = ringBuffer.next();
        try
        {
            final InputBufferEvent event = ringBuffer.get(sequence);
            event.setEventTime(requestTime);

            // Grow the event buffer in case of size larger than default.
            if (length > InputBufferEvent.DEFAULT_BUFFER_SIZE)
            {
                event.growBuffer(length);
            }

            event.getBuffer().putBytes(0, buffer, offset, length);
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
    }
}
