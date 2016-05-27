package org.helios.core.journal;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.core.journal.strategy.JournalStrategy;
import org.helios.core.journal.util.AllocationMode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.helios.core.journal.JournalRecordDescriptor.*;

public class JournalWriter implements MessageHandler, AutoCloseable
{
    private final RingBuffer nextRingBuffer;
    private final JournalStrategy journalStrategy;
    private final boolean flushing;
    private final ByteBuffer batchingBuffer;
    private long writeDuration;
    private long bytesWritten;
    private final IdleStrategy idleStrategy;

    public JournalWriter(final JournalStrategy journalStrategy, final AllocationMode allocationMode, final int pageSize,
        final boolean flushing, final RingBuffer nextRingBuffer, final IdleStrategy idleStrategy)
    {
        this.journalStrategy = journalStrategy;
        this.flushing = flushing;
        this.nextRingBuffer = nextRingBuffer;
        this.idleStrategy = idleStrategy;

        journalStrategy.open(allocationMode);

        batchingBuffer = ByteBuffer.allocate(pageSize); // TODO: can be improved with DirectBuffer?
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        try
        {
            final long start = System.nanoTime();

            final int messageSize = MESSAGE_FRAME_SIZE + length;

            journalStrategy.ensure(messageSize);

            if (batchingBuffer.remaining() < MESSAGE_HEADER_SIZE)
            {
                writeBatchingBuffer();
            }
            batchingBuffer.putInt(MESSAGE_HEAD);
            batchingBuffer.putInt(msgTypeId);
            batchingBuffer.putInt(length);

            for (int i = 0; i < length; i++)
            {
                if (!batchingBuffer.hasRemaining())
                {
                    writeBatchingBuffer();
                }

                batchingBuffer.put(buffer.getByte(index + i));
            }

            if (batchingBuffer.remaining() < MESSAGE_TRAILER_SIZE)
            {
                writeBatchingBuffer();
            }
            batchingBuffer.putInt(MESSAGE_TRAIL);

            writeDuration += (System.nanoTime() - start);
            bytesWritten += messageSize;

            // N.B. Technical Itch blog post 'Improving journalling latency' hints NOT TO USE flush.
            if (flushing)
            {
                writeBatchingBuffer();
                journalStrategy.flush();
            }
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            while (!nextRingBuffer.write(msgTypeId, buffer, index, length))
            {
                idleStrategy.idle(0);
            }
        }
    }

    @Override
    public void close() throws Exception
    {
        writeBatchingBuffer();
        journalStrategy.flush();

        CloseHelper.quietClose(journalStrategy);
    }

    @Override
    public String toString()
    {
        return String.format("%d bytesWritten, %d ms, rate %.02g bytes/sec", bytesWritten,
            TimeUnit.NANOSECONDS.toMillis(writeDuration),
            ((double) bytesWritten / (double) writeDuration) * 1_000_000_000);
    }

    private int writeBatchingBuffer() throws IOException
    {
        batchingBuffer.flip();
        int bytesWritten = journalStrategy.write(batchingBuffer);
        batchingBuffer.clear();

        return bytesWritten;
    }
}
