package echo;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.MessageTypes;
import org.helios.mmb.sbe.MMBHeaderTypeDecoder;
import org.helios.mmb.sbe.MessageHeaderDecoder;
import org.helios.mmb.sbe.SaveSnapshotDecoder;
import org.helios.service.ServiceHandler;
import org.helios.util.RingBufferPool;

public class EchoServiceHandler implements ServiceHandler
{
    private final RingBufferPool outputBufferPool;
    private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SaveSnapshotDecoder saveSnapshotDecoder = new SaveSnapshotDecoder();

    private long lastSnapshotTimestamp;

    public EchoServiceHandler(final RingBufferPool outputBufferPool)
    {
        this.outputBufferPool = outputBufferPool;
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        final RingBuffer outputBuffer = outputBufferPool.outputRingBuffers().iterator().next(); // FIXME: refactoring to avoid this API

        if (msgTypeId == MessageTypes.ADMINISTRATIVE_MSG_ID)
        {
            int bufferOffset = index;
            messageHeaderDecoder.wrap(buffer, bufferOffset);

            final int templateId = messageHeaderDecoder.templateId();
            if (templateId == SaveSnapshotDecoder.TEMPLATE_ID)
            {
                final int actingBlockLength = messageHeaderDecoder.blockLength();
                final int actingVersion = messageHeaderDecoder.version();

                bufferOffset += messageHeaderDecoder.encodedLength();

                saveSnapshotDecoder.wrap(buffer, bufferOffset, actingBlockLength, actingVersion);

                final MMBHeaderTypeDecoder mmbHeader = saveSnapshotDecoder.mmbHeader();
                final short nodeId = mmbHeader.nodeId();

                if (nodeId == 0)
                {
                    /* Save data snapshot: for ECHO SERVICE data is current time */
                    lastSnapshotTimestamp = System.nanoTime();
                }
            }
            else
            {
                /* Unexpected ADMINISTRATIVE message type, ignore */
            }
        }
        else if (msgTypeId == MessageTypes.APPLICATION_MSG_ID)
        {
            /* ECHO SERVICE message processing: reply the received message itself */

            while (!outputBuffer.write(MessageTypes.APPLICATION_MSG_ID, buffer, index, length))
            {
                idleStrategy.idle(0);
            }
        }
        else
        {
            /* Unexpected message type, ignore */
        }
    }

    @Override
    public void close() throws Exception
    {
    }

    public long lastSnapshotTimestamp()
    {
        return lastSnapshotTimestamp;
    }
}
