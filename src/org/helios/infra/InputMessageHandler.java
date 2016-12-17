package org.helios.infra;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.mmb.sbe.HeartbeatDecoder;
import org.helios.mmb.sbe.MessageHeaderDecoder;
import org.helios.mmb.sbe.ShutdownDecoder;
import org.helios.mmb.sbe.StartupDecoder;

import static org.agrona.UnsafeAccess.UNSAFE;

class InputMessageHandler implements FragmentHandler
{
    private static final long HEARTBEAT_RECEIVED_OFFSET;
    private static final long ADMINISTRATIVE_MESSAGES_OFFSET;
    private static final long APPLICATION_MESSAGES_OFFSET;
    private static final long BYTES_READ_OFFSET;

    private volatile long heartbeatReceived = 0;
    private volatile long administrativeMessages = 0;
    private volatile long applicationMessages = 0;
    private volatile long bytesRead = 0;

    private final RingBuffer inputRingBuffer;
    private final IdleStrategy idleStrategy;
    private final AssociationHandler associationHandler;
    private final MessageHeaderDecoder messageHeaderDecoder;
    private final StartupDecoder startupDecoder;
    private final ShutdownDecoder shutdownDecoder;

    InputMessageHandler(final RingBuffer inputRingBuffer, final IdleStrategy idleStrategy,
        final AssociationHandler associationHandler)
    {
        this.inputRingBuffer = inputRingBuffer;
        this.idleStrategy = idleStrategy;
        this.associationHandler = associationHandler;

        messageHeaderDecoder = new MessageHeaderDecoder();
        startupDecoder = new StartupDecoder();
        shutdownDecoder = new ShutdownDecoder();
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        if (templateId == HeartbeatDecoder.TEMPLATE_ID)
        {
            UNSAFE.putOrderedLong(this, ADMINISTRATIVE_MESSAGES_OFFSET, administrativeMessages + 1);
            UNSAFE.putOrderedLong(this, HEARTBEAT_RECEIVED_OFFSET, heartbeatReceived + 1);
        }
        else if (templateId == StartupDecoder.TEMPLATE_ID && associationHandler != null)
        {
            UNSAFE.putOrderedLong(this, ADMINISTRATIVE_MESSAGES_OFFSET, administrativeMessages + 1);

            offset += messageHeaderDecoder.encodedLength();

            startupDecoder.wrap(buffer, offset, messageHeaderDecoder.blockLength(), messageHeaderDecoder.version());

            // TODO: add component type/identifier to AssociationHandler hooks

            associationHandler.onAssociationEstablished();
        }
        else if (templateId == ShutdownDecoder.TEMPLATE_ID && associationHandler != null)
        {
            UNSAFE.putOrderedLong(this, ADMINISTRATIVE_MESSAGES_OFFSET, administrativeMessages + 1);

            offset += messageHeaderDecoder.encodedLength();

            shutdownDecoder.wrap(buffer, offset, messageHeaderDecoder.blockLength(), messageHeaderDecoder.version());

            // TODO: add component type/identifier to AssociationHandler hooks

            associationHandler.onAssociationBroken();
        }
        else
        {
            UNSAFE.putOrderedLong(this, APPLICATION_MESSAGES_OFFSET, applicationMessages + 1);
            
            while (!inputRingBuffer.write(MessageTypes.APPLICATION_MSG_ID, buffer, offset, length))
            {
                idleStrategy.idle(0);
            }
        }

        UNSAFE.putOrderedLong(this, BYTES_READ_OFFSET, bytesRead + length);
    }

    long heartbeatReceived()
    {
        return heartbeatReceived;
    }

    long administrativeMessages()
    {
        return administrativeMessages;
    }

    long applicationMessages()
    {
        return applicationMessages;
    }

    long bytesRead()
    {
        return bytesRead;
    }

    static
    {
        try
        {
            HEARTBEAT_RECEIVED_OFFSET = UNSAFE.objectFieldOffset(InputMessageHandler.class.getDeclaredField("heartbeatReceived"));
            ADMINISTRATIVE_MESSAGES_OFFSET = UNSAFE.objectFieldOffset(InputMessageHandler.class.getDeclaredField("administrativeMessages"));
            APPLICATION_MESSAGES_OFFSET = UNSAFE.objectFieldOffset(InputMessageHandler.class.getDeclaredField("applicationMessages"));
            BYTES_READ_OFFSET = UNSAFE.objectFieldOffset(InputMessageHandler.class.getDeclaredField("bytesRead"));
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
