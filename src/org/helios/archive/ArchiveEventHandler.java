package org.helios.archive;

import com.lmax.disruptor.EventHandler;
import org.agrona.CloseHelper;
import org.agrona.Verify;
import org.helios.util.Check;

import java.lang.reflect.Array;
import java.util.Arrays;

public class ArchiveEventHandler<E> implements EventHandler<E>, AutoCloseable
{
    private final E[] batch;
    private final ArchiveBatchHandler<E> batchHandler;
    private int batchIndex;

    @SuppressWarnings("unchecked")
    ArchiveEventHandler(final Class<E> eventClass, final int batchSize, final ArchiveBatchHandler<E> batchHandler)
    {
        Verify.notNull(eventClass, "eventClass");
        Verify.notNull(batchHandler, "eventClass");
        Check.enforce(batchSize > 0, "Non-positive batchSize");

        this.batchHandler = batchHandler;

        batch = (E[])Array.newInstance(eventClass, batchSize);
    }

    @Override
    public void onEvent(final E event, long sequence, boolean endOfBatch) throws Exception
    {
        batch[batchIndex] = event;
        batchIndex++;

        if (batchIndex == batch.length || endOfBatch)
        {
            batchHandler.onBatch(batch, 0, batchIndex);

            Arrays.fill(batch, 0, batchIndex, null);
            batchIndex = 0;
        }
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(batchHandler);
    }
}
