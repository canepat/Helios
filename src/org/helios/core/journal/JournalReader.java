package org.helios.core.journal;

import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.Verify;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.helios.util.DirectBufferAllocator;

import java.nio.ByteBuffer;

import static org.helios.core.journal.JournalRecordDescriptor.*;

public class JournalReader implements AutoCloseable
{
    private final Journalling journalling;
    private final MutableDirectBuffer readBuffer1;
    private final MutableDirectBuffer readBuffer2;
    private MutableDirectBuffer currentReadBuffer;

    public JournalReader(final Journalling journalling)
    {
        Verify.notNull(journalling, "journalling");

        this.journalling = journalling;

        final int pageSize = journalling.pageSize();
        readBuffer1 = new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(pageSize));
        readBuffer2 = new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(pageSize));

        currentReadBuffer = readBuffer1;
    }

    public int readFully(final MessageHandler handler) throws Exception
    {
        Verify.notNull(handler, "handler");

        int messagesRead = 0;

        int bytesRead;
        boolean lastJournalReached;
        boolean nextBlockWillRoll;
        do
        {
            final ByteBuffer buffer = currentReadBuffer.byteBuffer();

            bytesRead = journalling.read(buffer);

            buffer.flip();

            lastJournalReached = journalling.nextJournalNumber() == 0;
            nextBlockWillRoll = journalling.position() + currentReadBuffer.capacity() > journalling.size();

            while (buffer.remaining() > 0)
            {
                if (buffer.remaining() < MESSAGE_HEADER_SIZE)
                {
                    overflowReadBuffers();
                    break;
                }

                final int msgHead = buffer.getInt();
                if (msgHead != MESSAGE_HEAD)
                {
                    return messagesRead;
                }

                final int msgTypeId = buffer.getInt();
                final int length = buffer.getInt();

                if (buffer.remaining() < length + MESSAGE_TRAILER_SIZE)
                {
                    buffer.position(buffer.position() - MESSAGE_HEADER_SIZE);
                    overflowReadBuffers();
                    break;
                }

                final int messagePosition = buffer.position();
                handler.onMessage(msgTypeId, currentReadBuffer, messagePosition, length);
                buffer.position(messagePosition + length);

                int checkSum = 0;
                for (int i=0; i<length; i++)
                {
                    checkSum += currentReadBuffer.getByte(messagePosition + i);
                }
                final int msgCheckSum = buffer.getInt();
                if (checkSum != msgCheckSum)
                {
                    throw new IllegalStateException("Invalid checksum: expected=" + checkSum + " got=" + msgCheckSum);
                }

                final int msgTail = buffer.getInt();
                if (msgTail != MESSAGE_TRAIL)
                {
                    return messagesRead;
                }

                messagesRead++;
            }

            buffer.clear();
        }
        while (bytesRead != -1 && (!lastJournalReached || !nextBlockWillRoll));

        readBuffer1.byteBuffer().clear();
        readBuffer2.byteBuffer().clear();

        return messagesRead;
    }

    @Override
    public void close() throws Exception
    {
        CloseHelper.quietClose(journalling);
        DirectBufferAllocator.free(readBuffer1.byteBuffer());
        DirectBufferAllocator.free(readBuffer2.byteBuffer());
    }

    private void overflowReadBuffers()
    {
        final MutableDirectBuffer backupReadBuffer = (currentReadBuffer == readBuffer1) ? readBuffer2 : readBuffer1;

        backupReadBuffer.byteBuffer().put(currentReadBuffer.byteBuffer());
        currentReadBuffer.byteBuffer().clear();

        currentReadBuffer = (currentReadBuffer == readBuffer1) ? readBuffer2 : readBuffer1;
    }
}
