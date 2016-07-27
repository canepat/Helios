package org.helios.replica;

import io.aeron.Publication;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.AeronStream;

import static org.agrona.UnsafeAccess.UNSAFE;

public class ReplicaHandler implements MessageHandler, AutoCloseable
{
    private static final long MSG_REPLICATED_OFFSET;
    private static final long BYTES_REPLICATED_OFFSET;

    private volatile long msgReplicated = 0;
    private volatile long bytesReplicated = 0;

    private final RingBuffer nextRingBuffer;
    private final Publication replicaPublication;
    private final IdleStrategy idleStrategy;

    public ReplicaHandler(final RingBuffer nextRingBuffer, final IdleStrategy idleStrategy, final AeronStream replicaStream)
    {
        this.nextRingBuffer = nextRingBuffer;
        this.idleStrategy = idleStrategy;

        replicaPublication = replicaStream.aeron.addPublication(replicaStream.channel, replicaStream.streamId);
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        try
        {
            while (replicaPublication.offer(buffer, index, length) < 0L)
            {
                idleStrategy.idle(0);
            }

            UNSAFE.putOrderedLong(this, MSG_REPLICATED_OFFSET, msgReplicated + 1);
            UNSAFE.putOrderedLong(this, BYTES_REPLICATED_OFFSET, bytesReplicated + length);
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
        CloseHelper.quietClose(replicaPublication);
    }

    public long msgReplicated()
    {
        return msgReplicated;
    }

    public long bytesReplicated()
    {
        return bytesReplicated;
    }

    static
    {
        try
        {
            MSG_REPLICATED_OFFSET = UNSAFE.objectFieldOffset(ReplicaHandler.class.getDeclaredField("msgReplicated"));
            BYTES_REPLICATED_OFFSET = UNSAFE.objectFieldOffset(ReplicaHandler.class.getDeclaredField("bytesReplicated"));
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
