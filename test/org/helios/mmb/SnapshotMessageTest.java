package org.helios.mmb;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.MessageTypes;
import org.helios.mmb.SnapshotMessage;
import org.helios.mmb.sbe.*;
import org.helios.util.DirectBufferAllocator;
import org.junit.Test;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.junit.Assert.assertTrue;

public class SnapshotMessageTest
{
    private final int BUFFER_SIZE = (16 * 1024) + TRAILER_LENGTH;

    private final RingBuffer ringBuffer = new OneToOneRingBuffer(
        new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(BUFFER_SIZE)));

    private final SnapshotMessage snapshotMessage = new SnapshotMessage();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final LoadSnapshotDecoder loadSnapshotDecoder = new LoadSnapshotDecoder();
    private final SaveSnapshotDecoder saveSnapshotDecoder = new SaveSnapshotDecoder();

    @Test
    public void shouldWriteLoadSnapshotMessage()
    {
        snapshotMessage.writeLoadMessage(ringBuffer, new BusySpinIdleStrategy());

        int readBytes;
        do
        {
            readBytes = ringBuffer.read((msgTypeId, buffer, index, length) -> {
                assertTrue(msgTypeId == MessageTypes.ADMINISTRATIVE_MSG_ID);

                int bufferOffset = index;
                messageHeaderDecoder.wrap(buffer, bufferOffset);

                final int templateId = messageHeaderDecoder.templateId();
                assertTrue(templateId == LoadSnapshotDecoder.TEMPLATE_ID);

                final int actingBlockLength = messageHeaderDecoder.blockLength();
                final int actingVersion = messageHeaderDecoder.version();

                bufferOffset += messageHeaderDecoder.encodedLength();

                loadSnapshotDecoder.wrap(buffer, bufferOffset, actingBlockLength, actingVersion);

                final MMBHeaderTypeDecoder mmbHeader = loadSnapshotDecoder.mmbHeader();
                final short nodeId = mmbHeader.nodeId();

                assertTrue(nodeId == 0);
            });
        }
        while (readBytes == 0);
    }

    @Test
    public void shouldWriteSaveSnapshotMessage()
    {
        snapshotMessage.writeSaveMessage(ringBuffer, new BusySpinIdleStrategy());

        int readBytes;
        do
        {
            readBytes = ringBuffer.read((msgTypeId, buffer, index, length) -> {
                assertTrue(msgTypeId == MessageTypes.ADMINISTRATIVE_MSG_ID);

                int bufferOffset = index;
                messageHeaderDecoder.wrap(buffer, bufferOffset);

                final int templateId = messageHeaderDecoder.templateId();
                assertTrue(templateId == SaveSnapshotDecoder.TEMPLATE_ID);

                final int actingBlockLength = messageHeaderDecoder.blockLength();
                final int actingVersion = messageHeaderDecoder.version();

                bufferOffset += messageHeaderDecoder.encodedLength();

                saveSnapshotDecoder.wrap(buffer, bufferOffset, actingBlockLength, actingVersion);

                final MMBHeaderTypeDecoder mmbHeader = saveSnapshotDecoder.mmbHeader();
                final short nodeId = mmbHeader.nodeId();

                assertTrue(nodeId == 0);
            });
        }
        while (readBytes == 0);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenRingBufferIsNullInLoad()
    {
        snapshotMessage.writeLoadMessage(null, new BusySpinIdleStrategy());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenRingBufferIsNullInSave()
    {
        snapshotMessage.writeSaveMessage(null, new BusySpinIdleStrategy());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenIdleStrategyIsNullInLoad()
    {
        snapshotMessage.writeLoadMessage(
            new OneToOneRingBuffer(new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(BUFFER_SIZE))),
            null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenIdleStrategyIsNullInSave()
    {
        snapshotMessage.writeSaveMessage(
            new OneToOneRingBuffer(new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(BUFFER_SIZE))),
            null);
    }
}
