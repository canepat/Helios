package org.helios.mmb;

import org.agrona.concurrent.UnsafeBuffer;
import org.helios.infra.MessageTypes;
import org.helios.infra.OutputMessageHandler;
import org.helios.mmb.sbe.ComponentType;
import org.helios.mmb.sbe.HeartbeatEncoder;
import org.helios.mmb.sbe.MessageHeaderEncoder;
import org.helios.util.DirectBufferAllocator;

public final class HeartbeatMessage
{
    private static final int MESSAGE_LENGTH = 152;

    private final UnsafeBuffer heartbeatBuffer;
    private final int bufferOffset;
    private final HeartbeatEncoder heartbeatEncoder;

    public HeartbeatMessage(final ComponentType componentType, final short componentId)
    {
        heartbeatBuffer = new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(MESSAGE_LENGTH));
        heartbeatEncoder = new HeartbeatEncoder();

        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

        // Encode the HeartbeatMessage message once and forever
        int bufferOffset = 0;

        messageHeaderEncoder.wrap(heartbeatBuffer, bufferOffset)
            .blockLength(heartbeatEncoder.sbeBlockLength())
            .templateId(heartbeatEncoder.sbeTemplateId())
            .schemaId(heartbeatEncoder.sbeSchemaId())
            .version(heartbeatEncoder.sbeSchemaVersion());

        bufferOffset += messageHeaderEncoder.encodedLength();

        heartbeatEncoder.wrap(heartbeatBuffer, bufferOffset)
            .mmbHeader()
                .nodeId((short)0)
                .component()
                    .componentId(componentId)
                    .componentType(componentType);

        this.bufferOffset = bufferOffset;
    }

    public void write(final OutputMessageHandler outputMessageHandler)
    {
        heartbeatEncoder.wrap(heartbeatBuffer, bufferOffset)
            .mmbHeader()
                .timestamp(System.nanoTime());

        outputMessageHandler.onMessage(MessageTypes.ADMINISTRATIVE_MSG_ID, heartbeatBuffer, 0, MESSAGE_LENGTH);
    }
}
