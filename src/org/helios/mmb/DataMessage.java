package org.helios.mmb;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.helios.mmb.sbe.*;
import org.helios.util.Check;
import org.helios.util.DirectBufferAllocator;

public final class DataMessage
{
    private static final int MESSAGE_LENGTH = 152;

    private final UnsafeBuffer dataBuffer;
    private int dataBufferOffset;
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final DataEncoder dataEncoder = new DataEncoder();
    private final DataDecoder dataDecoder = new DataDecoder();

    public DataMessage()
    {
        dataBuffer = new UnsafeBuffer(0, 0);
        dataBufferOffset = 0;
    }

    public DataEncoder allocate(final ComponentType componentType, final short componentId, final int dataLength)
    {
        Check.enforce(dataLength >= 0, "Negative dataBufferLength");

        dataBuffer.wrap(DirectBufferAllocator.allocateCacheAligned(MESSAGE_LENGTH + dataLength));

        int bufferOffset = 0;

        messageHeaderEncoder.wrap(dataBuffer, bufferOffset)
            .blockLength(dataEncoder.sbeBlockLength())
            .templateId(dataEncoder.sbeTemplateId())
            .schemaId(dataEncoder.sbeSchemaId())
            .version(dataEncoder.sbeSchemaVersion());

        bufferOffset += messageHeaderEncoder.encodedLength();

        dataEncoder.wrap(dataBuffer, bufferOffset)
            .mmbHeader()
            .nodeId((short)0)
            .component()
            .componentId(componentId)
            .componentType(componentType);

        dataBufferOffset = bufferOffset;

        return dataEncoder;
    }

    public DataDecoder wrap(final MutableDirectBuffer buffer, final int offset, final int length)
    {
        dataBuffer.wrap(buffer, offset, length);

        int bufferOffset = 0;

        messageHeaderDecoder.wrap(dataBuffer, bufferOffset);

        final int actingBlockLength = messageHeaderDecoder.blockLength();
        final int actingVersion = messageHeaderDecoder.version();

        bufferOffset += messageHeaderDecoder.encodedLength();

        dataDecoder.wrap(dataBuffer, bufferOffset, actingBlockLength, actingVersion);

        dataBufferOffset = bufferOffset;

        return dataDecoder;
    }

    public UnsafeBuffer dataBuffer()
    {
        return dataBuffer;
    }

    public int dataBufferOffset()
    {
        return dataBufferOffset;
    }

    public int dataBufferLength()
    {
        return dataBuffer.capacity() - MESSAGE_LENGTH;
    }
}
