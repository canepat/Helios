package org.helios.core.snapshot;

import org.agrona.Verify;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.core.MessageTypes;
import org.helios.mmb.sbe.LoadSnapshotEncoder;
import org.helios.mmb.sbe.MessageHeaderEncoder;
import org.helios.mmb.sbe.SaveSnapshotEncoder;
import org.helios.util.DirectBufferAllocator;

public final class Snapshot
{
    private static final int MESSAGE_LENGTH = 128;

    private static final UnsafeBuffer saveSnapshotBuffer = new UnsafeBuffer(
        DirectBufferAllocator.allocateCacheAligned(MESSAGE_LENGTH));
    private static final UnsafeBuffer loadSnapshotBuffer = new UnsafeBuffer(
        DirectBufferAllocator.allocateCacheAligned(MESSAGE_LENGTH));

    public static void writeLoadMessage(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy)
    {
        Verify.notNull(idleStrategy, "idleStrategy");

        while (!inputRingBuffer.write(MessageTypes.ADMINISTRATIVE_MSG_ID, loadSnapshotBuffer, 0, MESSAGE_LENGTH))
        {
            idleStrategy.idle(0);
        }
    }

    public static void writeSaveMessage(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy)
    {
        Verify.notNull(idleStrategy, "idleStrategy");

        while (!inputRingBuffer.write(MessageTypes.ADMINISTRATIVE_MSG_ID, saveSnapshotBuffer, 0, MESSAGE_LENGTH))
        {
            idleStrategy.idle(0);
        }
    }

    static
    {
        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

        // Encode the Load Data Snapshot message once and forever
        final LoadSnapshotEncoder loadSnapshotEncoder = new LoadSnapshotEncoder();
        int bufferOffset = 0;

        messageHeaderEncoder.wrap(loadSnapshotBuffer, bufferOffset)
            .blockLength(loadSnapshotEncoder.sbeBlockLength())
            .templateId(loadSnapshotEncoder.sbeTemplateId())
            .schemaId(loadSnapshotEncoder.sbeSchemaId())
            .version(loadSnapshotEncoder.sbeSchemaVersion());

        bufferOffset += messageHeaderEncoder.encodedLength();

        loadSnapshotEncoder.wrap(loadSnapshotBuffer, bufferOffset)
            .mMBHeader()
            .messageId(0L)
            .nodeId((short)0)
            .timestamp(System.nanoTime());

        // Encode the Save Data Snapshot message once and forever
        final SaveSnapshotEncoder saveSnapshotEncoder = new SaveSnapshotEncoder();
        bufferOffset = 0;

        messageHeaderEncoder.wrap(saveSnapshotBuffer, bufferOffset)
            .blockLength(saveSnapshotEncoder.sbeBlockLength())
            .templateId(saveSnapshotEncoder.sbeTemplateId())
            .schemaId(saveSnapshotEncoder.sbeSchemaId())
            .version(saveSnapshotEncoder.sbeSchemaVersion());

        bufferOffset += messageHeaderEncoder.encodedLength();

        saveSnapshotEncoder.wrap(saveSnapshotBuffer, bufferOffset)
            .mMBHeader()
            .messageId(1L)
            .nodeId((short)0)
            .timestamp(System.nanoTime());
    }
}
