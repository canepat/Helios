package org.helios.core.journal;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.core.MessageTypes;
import org.helios.core.journal.strategy.JournalStrategy;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class JournalHandler implements MessageHandler, AutoCloseable
{
    private static final int PAGE_SIZE = Integer.getInteger("helios.core.journal.page_size", 4 * 1024);
    private static final int BUFFER_LENGTH_SIZE = 4;

    private final RingBuffer nextRingBuffer;
    private final JournalStrategy journalStrategy;
    private final boolean flushing;
    private final ByteBuffer batchingBuffer;
    private long writeDuration;
    private long bytesWritten;
    private final IdleStrategy idleStrategy;

    public JournalHandler(final JournalStrategy journalStrategy, final boolean flushing, final RingBuffer nextRingBuffer,
        final IdleStrategy idleStrategy)
    {
        this.journalStrategy = journalStrategy;
        this.flushing = flushing;
        this.nextRingBuffer = nextRingBuffer;
        this.idleStrategy = idleStrategy;

        journalStrategy.open();

        batchingBuffer = ByteBuffer.allocate(PAGE_SIZE); // TODO: can be improved with DirectBuffer?
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        try
        {
            final long start = System.nanoTime();

            if (batchingBuffer.remaining() < BUFFER_LENGTH_SIZE)
            {
                batchingBuffer.flip();
                journalStrategy.write(batchingBuffer);
                batchingBuffer.clear();
            }
            batchingBuffer.putInt(length);

            for (int i = 0; i < length; i++)
            {
                if (!batchingBuffer.hasRemaining())
                {
                    batchingBuffer.flip();
                    journalStrategy.write(batchingBuffer);
                    batchingBuffer.clear();
                }

                batchingBuffer.put(buffer.getByte(index + i));
            }

            writeDuration += (System.nanoTime() - start);
            bytesWritten += (BUFFER_LENGTH_SIZE + length);

            // Flush if enabled.
            // TODO: Technical Itch blog 'Improving journalling latency' hints NOT TO USE flush
            if (flushing)
            {
                journalStrategy.flush();
            }
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            while (!nextRingBuffer.write(MessageTypes.APPLICATION_MSG_ID, buffer, index, length))
            {
                idleStrategy.idle(0);
            }
        }
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
