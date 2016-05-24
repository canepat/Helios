package org.helios.infra;

import io.aeron.Publication;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.helios.AeronStream;

import static org.agrona.UnsafeAccess.UNSAFE;

public class OutputMessageHandler implements MessageHandler, AutoCloseable
{
    private static final long SUCCESSFUL_WRITES_OFFSET;
    private static final long FAILED_WRITES_OFFSET;

    private volatile long successfulWrites = 0;
    private volatile long failedWrites = 0;

    private final Publication outputPublication;
    private final IdleStrategy idleStrategy;

    public OutputMessageHandler(final AeronStream outputStream, final IdleStrategy idleStrategy)
    {
        this.idleStrategy = idleStrategy;

        outputPublication = outputStream.aeron.addPublication(outputStream.channel, outputStream.streamId);
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        try
        {
            while (outputPublication.offer(buffer, index, length) < 0L)
            {
                idleStrategy.idle(0);
            }

            UNSAFE.putOrderedLong(this, SUCCESSFUL_WRITES_OFFSET, successfulWrites + 1);
            UNSAFE.putOrderedLong(this, FAILED_WRITES_OFFSET, failedWrites + length);
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(outputPublication);
    }

    public long successfulWrites()
    {
        return successfulWrites;
    }

    public long failedWrites()
    {
        return failedWrites;
    }

    static
    {
        try
        {
            SUCCESSFUL_WRITES_OFFSET = UNSAFE.objectFieldOffset(OutputMessageHandler.class.getDeclaredField("successfulWrites"));
            FAILED_WRITES_OFFSET = UNSAFE.objectFieldOffset(OutputMessageHandler.class.getDeclaredField("failedWrites"));
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
