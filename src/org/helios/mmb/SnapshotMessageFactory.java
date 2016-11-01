package org.helios.mmb;

import org.agrona.concurrent.UnsafeBuffer;
import org.helios.mmb.sbe.LoadSnapshotEncoder;
import org.helios.mmb.sbe.MessageHeaderEncoder;
import org.helios.mmb.sbe.SaveSnapshotEncoder;
import org.helios.util.DirectBufferAllocator;

public final class SnapshotMessageFactory
{
    public static final int MESSAGE_LENGTH = 128;

    public static final UnsafeBuffer saveSnapshotBuffer;
    public static final UnsafeBuffer loadSnapshotBuffer;

    static
    {
        saveSnapshotBuffer = new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(MESSAGE_LENGTH));
        loadSnapshotBuffer = new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(MESSAGE_LENGTH));

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
            .mmbHeader()
            .messageNumber(0L)
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
            .mmbHeader()
            .messageNumber(0L)
            .nodeId((short)0)
            .timestamp(System.nanoTime());
    }
}
