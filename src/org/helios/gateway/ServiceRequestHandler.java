package org.helios.gateway;

import io.aeron.Publication;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.helios.AeronStream;

import static org.agrona.UnsafeAccess.UNSAFE;

public class ServiceRequestHandler implements MessageHandler, AutoCloseable
{
    private static final long MSG_REQUESTED_OFFSET;
    private static final long BYTES_REQUESTED_OFFSET;

    private volatile long msgRequested = 0;
    private volatile long bytesRequested = 0;

    private final Publication servicePublication;
    private final IdleStrategy idleStrategy;

    public ServiceRequestHandler(final AeronStream reqStream, final IdleStrategy idleStrategy)
    {
        this.idleStrategy = idleStrategy;

        servicePublication = reqStream.aeron.addPublication(reqStream.channel, reqStream.streamId);
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        try
        {
            while (servicePublication.offer(buffer, index, length) < 0L)
            {
                idleStrategy.idle(0);
            }

            UNSAFE.putOrderedLong(this, MSG_REQUESTED_OFFSET, msgRequested + 1);
            UNSAFE.putOrderedLong(this, BYTES_REQUESTED_OFFSET, bytesRequested + length);
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(servicePublication);
    }

    public long msgRequested()
    {
        return msgRequested;
    }

    public long bytesRequested()
    {
        return bytesRequested;
    }

    static
    {
        try
        {
            MSG_REQUESTED_OFFSET = UNSAFE.objectFieldOffset(ServiceRequestHandler.class.getDeclaredField("msgRequested"));
            BYTES_REQUESTED_OFFSET = UNSAFE.objectFieldOffset(ServiceRequestHandler.class.getDeclaredField("bytesRequested"));
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
