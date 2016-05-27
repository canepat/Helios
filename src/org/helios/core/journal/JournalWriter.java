package org.helios.core.journal;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.helios.core.journal.util.AllocationMode;
import org.helios.util.DirectBufferAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.helios.core.journal.JournalRecordDescriptor.*;

public class JournalWriter implements MessageHandler, AutoCloseable
{
    private final Journalling journalling;
    private final boolean flushing;
    private final ByteBuffer batchingBuffer;
    private long writeDuration;
    private long bytesWritten;

    public JournalWriter(final Journalling journalling, final int pageSize, final boolean flushing)
    {
        this.journalling = journalling;
        this.flushing = flushing;

        journalling.open(AllocationMode.ZEROED_ALLOCATION);

        batchingBuffer = DirectBufferAllocator.allocateCacheAligned(pageSize);
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        try
        {
            final long start = System.nanoTime();

            final int messageSize = MESSAGE_FRAME_SIZE + length;

            journalling.ensure(messageSize);

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
                journalling.flush();
            }
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    @Override
    public void close() throws Exception
    {
        writeBatchingBuffer();
        journalling.flush();

        CloseHelper.quietClose(journalling);
        DirectBufferAllocator.free(batchingBuffer);
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
        int bytesWritten = journalling.write(batchingBuffer);
        batchingBuffer.clear();

        return bytesWritten;
    }
}
