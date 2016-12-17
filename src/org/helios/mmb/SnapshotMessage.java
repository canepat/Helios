package org.helios.mmb;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.MessageTypes;
import org.helios.mmb.sbe.LoadSnapshotEncoder;
import org.helios.mmb.sbe.MessageHeaderEncoder;
import org.helios.mmb.sbe.SaveSnapshotEncoder;
import org.helios.util.DirectBufferAllocator;

import java.util.Objects;

public final class SnapshotMessage
{
    private static final int MESSAGE_LENGTH = 152;

    private final UnsafeBuffer loadSnapshotBuffer;
    private final UnsafeBuffer saveSnapshotBuffer;
    private final LoadSnapshotEncoder loadSnapshotEncoder = new LoadSnapshotEncoder();
    private final SaveSnapshotEncoder saveSnapshotEncoder = new SaveSnapshotEncoder();
    private final int loadBufferOffset;
    private final int saveBufferOffset;

    public SnapshotMessage()
    {
        loadSnapshotBuffer = new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(MESSAGE_LENGTH));
        saveSnapshotBuffer = new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(MESSAGE_LENGTH));

        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

        // Encode the Load Data SnapshotMessage message once and forever
        int bufferOffset = 0;

        messageHeaderEncoder.wrap(loadSnapshotBuffer, bufferOffset)
            .blockLength(loadSnapshotEncoder.sbeBlockLength())
            .templateId(loadSnapshotEncoder.sbeTemplateId())
            .schemaId(loadSnapshotEncoder.sbeSchemaId())
            .version(loadSnapshotEncoder.sbeSchemaVersion());

        bufferOffset += messageHeaderEncoder.encodedLength();

        loadSnapshotEncoder.wrap(loadSnapshotBuffer, bufferOffset)
            .mmbHeader()
            .messageNumber(0L)
            .nodeId((short)0)
            .timestamp(System.nanoTime());

        loadBufferOffset = bufferOffset;

        // Encode the Save Data SnapshotMessage message once and forever
        bufferOffset = 0;

        messageHeaderEncoder.wrap(saveSnapshotBuffer, bufferOffset)
            .blockLength(saveSnapshotEncoder.sbeBlockLength())
            .templateId(saveSnapshotEncoder.sbeTemplateId())
            .schemaId(saveSnapshotEncoder.sbeSchemaId())
            .version(saveSnapshotEncoder.sbeSchemaVersion());

        bufferOffset += messageHeaderEncoder.encodedLength();

        saveSnapshotEncoder.wrap(saveSnapshotBuffer, bufferOffset)
            .mmbHeader()
            .messageNumber(0L)
            .nodeId((short)0)
            .timestamp(System.nanoTime());

        saveBufferOffset = bufferOffset;
    }

    public void writeLoadMessage(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy)
    {
        Objects.requireNonNull(idleStrategy, "idleStrategy");

        loadSnapshotEncoder.wrap(loadSnapshotBuffer, loadBufferOffset)
            .mmbHeader()
                .timestamp(System.nanoTime());

        while (!inputRingBuffer.write(MessageTypes.ADMINISTRATIVE_MSG_ID, loadSnapshotBuffer, 0, MESSAGE_LENGTH))
        {
            idleStrategy.idle(0);
        }
    }

    public void writeSaveMessage(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy)
    {
        Objects.requireNonNull(idleStrategy, "idleStrategy");

        saveSnapshotEncoder.wrap(saveSnapshotBuffer, saveBufferOffset)
            .mmbHeader()
                .timestamp(System.nanoTime());

        while (!inputRingBuffer.write(MessageTypes.ADMINISTRATIVE_MSG_ID, saveSnapshotBuffer, 0, MESSAGE_LENGTH))
        {
            idleStrategy.idle(0);
        }
    }
}
