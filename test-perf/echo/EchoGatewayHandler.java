package echo;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.core.MessageTypes;
import org.helios.gateway.GatewayHandler;
import org.helios.util.DirectBufferAllocator;

import static org.agrona.UnsafeAccess.UNSAFE;
import static echo.EchoConfiguration.MESSAGE_LENGTH;

public class EchoGatewayHandler implements GatewayHandler
{
    private static final long TIMESTAMP_OFFSET;

    private final RingBuffer outputBuffer;
    private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
    private final UnsafeBuffer echoBuffer = new UnsafeBuffer(DirectBufferAllocator.allocate(MESSAGE_LENGTH));

    private volatile long timestamp = 0;

    public EchoGatewayHandler(final RingBuffer outputBuffer)
    {
        this.outputBuffer = outputBuffer;
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        /* ECHO GATEWAY message processing: store last echo timestamp */

        final long echoTimestamp = buffer.getLong(index);

        UNSAFE.putOrderedLong(this, TIMESTAMP_OFFSET, echoTimestamp);
    }

    @Override
    public void close() throws Exception
    {
    }

    public void sendEcho(final long echoTimestamp)
    {
        echoBuffer.putLong(0, echoTimestamp);

        while (!outputBuffer.write(MessageTypes.APPLICATION_MSG_ID, echoBuffer, 0, MESSAGE_LENGTH))
        {
            idleStrategy.idle(0);
        }

        UNSAFE.putOrderedLong(this, TIMESTAMP_OFFSET, 0);
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    static
    {
        try
        {
            TIMESTAMP_OFFSET = UNSAFE.objectFieldOffset(EchoGatewayHandler.class.getDeclaredField("timestamp"));
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
