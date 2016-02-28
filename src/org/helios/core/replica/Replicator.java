package org.helios.core.replica;

import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceReportingEventHandler;
import org.helios.core.engine.InputBufferEvent;
import org.helios.mmb.AeronPublisher;
import org.helios.mmb.MMBPublisher;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.agrona.DirectBuffer;

public class Replicator implements SequenceReportingEventHandler<InputBufferEvent>, LifecycleAware, AutoCloseable
{
    private final MMBPublisher busPublisher;
    private Sequence lastSequence;

    public Replicator(final Aeron aeron)
    {
        String channel = System.getProperty("onm.ipc.mmb.replica_channel", "udp://localhost:40125");
        int streamId = Integer.getInteger("onm.ipc.mmb.replica_stream_id", 10);

        this.busPublisher = new AeronPublisher(aeron, channel, streamId);
    }

    @Override
    public void onEvent(final InputBufferEvent event, long sequence, boolean endOfBatch) throws Exception
    {
        final DirectBuffer buffer = event.getBuffer();
        //System.out.println("Replicator: buffer.array()=" + (buffer.limit() - buffer.position()));

        busPublisher.send(buffer, buffer.byteBuffer().position(), buffer.byteBuffer().remaining());

        lastSequence.set(sequence);
    }

    @Override
    public void setSequenceCallback(Sequence lastSequence)
    {
        this.lastSequence = lastSequence;
    }

    @Override
    public void onStart()
    {
        System.out.println("Replicator::onStart called");
    }

    @Override
    public void onShutdown()
    {
        System.out.println("Replicator::onShutdown called");
    }

    @Override
    public void close() throws Exception
    {
        busPublisher.close();
    }

    @Override
    public String toString()
    {
        return "Replicator: " + busPublisher;
    }
}
