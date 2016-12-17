package org.helios.infra;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.helios.AeronStream;
import org.helios.mmb.sbe.MessageHeaderDecoder;
import org.helios.mmb.sbe.ShutdownDecoder;

import static org.agrona.UnsafeAccess.UNSAFE;

public class OutputMessageHandler implements MessageHandler, AutoCloseable
{
    private static final long SUCCESSFUL_WRITES_OFFSET;
    private static final long FAILED_WRITES_OFFSET;
    private static final long BYTES_WRITTEN_OFFSET;

    private volatile long successfulWrites = 0;
    private volatile long failedWrites = 0;
    private volatile long bytesWritten = 0;

    private final Publication outputPublication;
    private final IdleStrategy idleStrategy;
    private final BufferClaim bufferClaim;

    OutputMessageHandler(final AeronStream outputStream, final IdleStrategy idleStrategy)
    {
        this.idleStrategy = idleStrategy;

        outputPublication = outputStream.aeron.addPublication(outputStream.channel, outputStream.streamId);

        bufferClaim = new BufferClaim();
    }
    private MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        try
        {
            messageHeaderDecoder.wrap(buffer, index);
            if (messageHeaderDecoder.templateId() == ShutdownDecoder.TEMPLATE_ID)
            {
                System.out.println("ShutdownDecoder.TEMPLATE_ID matched!");
            }

            while (outputPublication.tryClaim(length, bufferClaim) <= 0)
            {
                UNSAFE.putOrderedLong(this, FAILED_WRITES_OFFSET, failedWrites + 1);

                idleStrategy.idle(0);
            }

            final int offset = bufferClaim.offset();
            bufferClaim.buffer().putBytes(offset, buffer, index, length);
            bufferClaim.commit();

            UNSAFE.putOrderedLong(this, SUCCESSFUL_WRITES_OFFSET, successfulWrites + 1);
            UNSAFE.putOrderedLong(this, BYTES_WRITTEN_OFFSET, bytesWritten + length);
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

    public long outputPublicationId()
    {
        return this.outputPublication.registrationId();
    }

    long successfulWrites()
    {
        return successfulWrites;
    }

    long failedWrites()
    {
        return failedWrites;
    }

    long bytesWritten()
    {
        return bytesWritten;
    }

    static
    {
        try
        {
            SUCCESSFUL_WRITES_OFFSET = UNSAFE.objectFieldOffset(OutputMessageHandler.class.getDeclaredField("successfulWrites"));
            FAILED_WRITES_OFFSET = UNSAFE.objectFieldOffset(OutputMessageHandler.class.getDeclaredField("failedWrites"));
            BYTES_WRITTEN_OFFSET = UNSAFE.objectFieldOffset(OutputMessageHandler.class.getDeclaredField("bytesWritten"));
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
