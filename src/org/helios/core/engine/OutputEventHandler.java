package org.helios.core.engine;

import org.helios.util.DirectBufferAllocator;
import org.helios.mmb.MMBPublisher;
import com.lmax.disruptor.EventHandler;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class OutputEventHandler implements EventHandler<OutputBufferEvent>
{
    private static final int BATCHING_SIZE = Integer.getInteger("helios.core.engine.output_batching_size", 100);

    private final MMBPublisher busPublisher;
    private final MutableDirectBuffer[] batchingBuffers;
    private int batchingBufferIndex;

    public OutputEventHandler(final MMBPublisher busPublisher)
    {
        this.busPublisher = busPublisher;

        batchingBuffers = new MutableDirectBuffer[BATCHING_SIZE];
        for (int i=0; i<batchingBuffers.length; i++)
        {
            batchingBuffers[i] = new UnsafeBuffer(ByteBuffer.allocateDirect(OutputBufferEvent.DEFAULT_BUFFER_SIZE));
        }
    }

    @Override
    public void onEvent(final OutputBufferEvent event, long sequence, boolean endOfBatch) throws Exception
    {
        final DirectBuffer buffer = event.getBuffer();

        // Send the event buffer on the MMB.
        //busPublisher.send(buffer, buffer.byteBuffer().position(), buffer.byteBuffer().remaining());

        // TODO: enable batching buffer to exploit disruptor 'batching effect' [TBD: handle message framing!]
        if (batchingBufferIndex < BATCHING_SIZE || endOfBatch)
        {
            // TODO: multiple send message on publisher when supported by Aeron/JVM
            for (int i=0; i<batchingBufferIndex; i++)
            {
                final DirectBuffer batchingBuffer = batchingBuffers[i];
                busPublisher.send(batchingBuffer, batchingBuffer.byteBuffer().position(), batchingBuffer.byteBuffer().remaining());
            }
            busPublisher.send(buffer, buffer.byteBuffer().position(), buffer.byteBuffer().remaining());

            batchingBufferIndex = 0;
        }
        else
        {
            final MutableDirectBuffer batchingBuffer = batchingBuffers[batchingBufferIndex];

            // Handle the default batching buffer overflow.
            if (batchingBuffer.capacity() < buffer.capacity())
            {
                DirectBufferAllocator.freeDirect(batchingBuffer.byteBuffer());
                batchingBuffer.wrap(DirectBufferAllocator.allocateDirect(buffer.capacity()));
            }
            batchingBuffer.putBytes(0, buffer, 0, buffer.capacity());
            batchingBufferIndex++;
        }

        // Reset the default event buffer if size larger than default is in use.
        if (buffer.capacity() > OutputBufferEvent.DEFAULT_BUFFER_SIZE)
        {
            event.resetBuffer();
        }
    }
}
