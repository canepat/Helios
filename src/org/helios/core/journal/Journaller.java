package org.helios.core.journal;

import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceReportingEventHandler;
import org.helios.core.engine.InputBufferEvent;
import org.helios.core.journal.strategy.JournalStrategy;
import uk.co.real_logic.agrona.CloseHelper;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class Journaller implements SequenceReportingEventHandler<InputBufferEvent>, LifecycleAware, AutoCloseable
{
    private static final int PAGE_SIZE = Integer.getInteger("helios.core.journal.page_size", 4 * 1024);
    private static final int BUFFER_LENGTH_SIZE = 4;

    private final JournalStrategy journalStrategy;
    private final boolean flushing;
    //private final ByteBuffer lengthBuffer;
    private final ByteBuffer batchingBuffer;
    private Sequence lastSequence;
    private long writeDuration;
    private long bytesWritten;

    public Journaller(final JournalStrategy journalStrategy, boolean flushing)
    {
        this.journalStrategy = journalStrategy;
        this.flushing = flushing;

        //lengthBuffer = ByteBuffer.allocateDirect(BUFFER_LENGTH_SIZE);
        batchingBuffer = ByteBuffer.allocate(PAGE_SIZE); // TODO: can be improved with DirectBuffer?
    }

    @Override
    public void onEvent(final InputBufferEvent event, long sequence, boolean endOfBatch) throws Exception
    {
        final ByteBuffer buffer = event.getBuffer().byteBuffer(); // TODO: can be improved with DirectBuffer?
        final int size = buffer.remaining(); // remaining() ? was limit()
        //System.out.println("Journaller: size=" + buffer.limit() + " buffer=" + buffer);
/*
        // Write the buffer size on the batching buffer.
        lengthBuffer.clear();
        lengthBuffer.putInt(size).flip();

        // Write the buffer size and content delegating to strategy.
        journalStrategy.write(lengthBuffer);
        while (buffer.hasRemaining())
        {
            journalStrategy.write(buffer);
        }
*/
        boolean batchingOverflow = false;

        long start = System.nanoTime();

        if (batchingBuffer.remaining() < BUFFER_LENGTH_SIZE)
        {
            batchingBuffer.flip();
            journalStrategy.write(batchingBuffer);
            batchingBuffer.clear();

            batchingOverflow = true;
        }
        batchingBuffer.putInt(size);

        for (int i=0; i<size; i++)
        {
            if (!batchingBuffer.hasRemaining())
            {
                batchingBuffer.flip();
                journalStrategy.write(batchingBuffer);
                batchingBuffer.clear();

                batchingOverflow = true;
            }

            batchingBuffer.put(buffer.get());
        }

        writeDuration += (System.nanoTime() - start);
        bytesWritten += (BUFFER_LENGTH_SIZE + size);

        // Reset the buffer content for any following event handler.
        buffer.clear().limit(size);
        //System.out.println("Journaller: size=" + buffer.limit() + " buffer=" + buffer);

        // When the batching buffer has been filled, update the processor sequence back to let the producer advance.
        if (batchingOverflow)
        {
            lastSequence.set(sequence);
        }

        // Flush at the end of batch if enabled.
        // TODO: Technical Itch blog 'Improving journalling latency' hints NOT TO USE flush
        if (endOfBatch && flushing)
        {
            journalStrategy.flush();
        }
    }

    @Override
    public void setSequenceCallback(Sequence lastSequence)
    {
        this.lastSequence = lastSequence;
    }

    @Override
    public void onStart()
    {
    }

    @Override
    public void onShutdown()
    {
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(journalStrategy);
    }

    @Override
    public String toString()
    {
        return String.format("%d bytesWritten, %d ms, rate %.02g bytes/sec", bytesWritten,
            TimeUnit.NANOSECONDS.toMillis(writeDuration),
            ((double) bytesWritten / (double) writeDuration) * 1_000_000_000);
    }
}
