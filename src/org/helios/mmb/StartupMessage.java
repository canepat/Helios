package org.helios.mmb;

import org.agrona.concurrent.UnsafeBuffer;
import org.helios.infra.MessageTypes;
import org.helios.infra.OutputMessageHandler;
import org.helios.mmb.sbe.ComponentType;
import org.helios.mmb.sbe.MessageHeaderEncoder;
import org.helios.mmb.sbe.StartupEncoder;
import org.helios.util.DirectBufferAllocator;

public final class StartupMessage
{
    private static final int MESSAGE_LENGTH = 152;

    private final UnsafeBuffer startupBuffer;

    public StartupMessage(final ComponentType componentType, final short componentId)
    {
        startupBuffer = new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(MESSAGE_LENGTH));

        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        final StartupEncoder startupEncoder = new StartupEncoder();

        // Encode the StartupMessage message once and forever
        int bufferOffset = 0;

        messageHeaderEncoder.wrap(startupBuffer, bufferOffset)
            .blockLength(startupEncoder.sbeBlockLength())
            .templateId(startupEncoder.sbeTemplateId())
            .schemaId(startupEncoder.sbeSchemaId())
            .version(startupEncoder.sbeSchemaVersion());

        bufferOffset += messageHeaderEncoder.encodedLength();

        startupEncoder.wrap(startupBuffer, bufferOffset)
            .mmbHeader()
                .nodeId((short)0)
                .timestamp(System.nanoTime())
                .component()
                    .componentId(componentId)
                    .componentType(componentType);
    }

    public void write(final OutputMessageHandler outputMessageHandler)
    {
        outputMessageHandler.onMessage(MessageTypes.ADMINISTRATIVE_MSG_ID, startupBuffer, 0, MESSAGE_LENGTH);
    }
}
