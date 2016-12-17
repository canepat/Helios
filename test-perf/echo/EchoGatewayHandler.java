package echo;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.gateway.GatewayHandler;
import org.helios.infra.MessageTypes;
import org.helios.mmb.DataMessage;
import org.helios.mmb.sbe.ComponentType;
import org.helios.util.RingBufferPool;

import static echo.EchoConfiguration.MESSAGE_LENGTH;

public class EchoGatewayHandler implements GatewayHandler
{
    //private static final long TIMESTAMP_OFFSET;

    private final RingBufferPool outputBufferPool;
    private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
    //private final UnsafeBuffer echoBuffer = new UnsafeBuffer(DirectBufferAllocator.allocate(MESSAGE_LENGTH));
    private final DataMessage outgoingDataMessage = new DataMessage();
    private final DataMessage incomingDataMessage = new DataMessage();

    //private volatile long timestamp = 0;

    EchoGatewayHandler(final RingBufferPool outputBufferPool)
    {
        this.outputBufferPool = outputBufferPool;

        outgoingDataMessage.allocate(ComponentType.Gateway, (short)1, MESSAGE_LENGTH);
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        if (msgTypeId == MessageTypes.APPLICATION_MSG_ID)
        {
            incomingDataMessage.wrap(buffer, index, length);
            final UnsafeBuffer dataBuffer = incomingDataMessage.dataBuffer();
            final int dataOffset = incomingDataMessage.dataBufferOffset();

            /* ECHO GATEWAY message processing: store last echo timestamp */
            final long echoTimestamp = dataBuffer.getLong(dataOffset);

            //final long echoTimestamp = buffer.getLong(index);

            //UNSAFE.putOrderedLong(this, TIMESTAMP_OFFSET, echoTimestamp);

            //final long timestamp = timestampQueue.remove();
            //System.out.println("RECEIVED echoTimestamp=" + echoTimestamp);
        }
    }

    @Override
    public void close() throws Exception
    {
    }

    void sendEcho(final long echoTimestamp)
    {
        final RingBuffer outputBuffer = outputBufferPool.outputRingBuffers().iterator().next(); // FIXME: refactoring to avoid this API

        final UnsafeBuffer dataBuffer = outgoingDataMessage.dataBuffer();
        final int dataOffset = outgoingDataMessage.dataBufferOffset();

        //echoBuffer.putLong(0, echoTimestamp);
        dataBuffer.putLong(dataOffset, echoTimestamp);

        /*while (!outputBuffer.write(MessageTypes.APPLICATION_MSG_ID, echoBuffer, 0, MESSAGE_LENGTH))
        {
            idleStrategy.idle(0);
        }*/
        while (!outputBuffer.write(MessageTypes.APPLICATION_MSG_ID, dataBuffer, 0, dataBuffer.capacity()))
        {
            idleStrategy.idle(0);
        }

        //System.out.println("SENT echoTimestamp=" + echoTimestamp);
        //UNSAFE.putOrderedLong(this, TIMESTAMP_OFFSET, 0);
        //timestampQueue.add(echoTimestamp);
    }

    /*public long getTimestamp()
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
    }*/
}
