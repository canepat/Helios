package org.helios.mmb;

import org.agrona.concurrent.UnsafeBuffer;
import org.helios.mmb.sbe.HeartbeatEncoder;
import org.helios.mmb.sbe.MessageHeaderEncoder;
import org.helios.util.DirectBufferAllocator;

public final class HeartbeatMessageFactory
{
    public static final int MESSAGE_LENGTH = 128;

    public static final UnsafeBuffer heartbeatBuffer;

    static
    {
        heartbeatBuffer = new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(MESSAGE_LENGTH));

        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

        // Encode the Heartbeat message once and forever
        final HeartbeatEncoder heartbeatEncoder = new HeartbeatEncoder();
        int bufferOffset = 0;

        messageHeaderEncoder.wrap(heartbeatBuffer, bufferOffset)
            .blockLength(heartbeatEncoder.sbeBlockLength())
            .templateId(heartbeatEncoder.sbeTemplateId())
            .schemaId(heartbeatEncoder.sbeSchemaId())
            .version(heartbeatEncoder.sbeSchemaVersion());

        bufferOffset += messageHeaderEncoder.encodedLength();

        heartbeatEncoder.wrap(heartbeatBuffer, bufferOffset)
            .mmbHeader()
            .messageId(2L)
            .nodeId((short)0)
            .timestamp(System.nanoTime());
    }
}
