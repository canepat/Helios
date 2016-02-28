package org.helios.core.engine;

import com.lmax.disruptor.dsl.Disruptor;
import org.helios.mmb.sbe.MessageHeaderDecoder;
import uk.co.real_logic.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;

public abstract class BaseServiceHandler implements ServiceHandler
{
    private final Disruptor<OutputBufferEvent> outputDisruptor;
    private final MessageHeaderDecoder headerDecoder;
    private long serviceTime;
    private long serviceOperations;

    public BaseServiceHandler(final Disruptor<OutputBufferEvent> outputDisruptor)
    {
        this.outputDisruptor = outputDisruptor;

        headerDecoder = new MessageHeaderDecoder();
    }

    @Override
    public void onEvent(final InputBufferEvent event, long sequence, boolean endOfBatch) throws Exception
    {
        try
        {
            final long start = System.nanoTime();

            final DirectBuffer decodeBuffer = event.getBuffer();

            // Wrap the message header decoder around for decoding the message buffer.
            headerDecoder.wrap(decodeBuffer, 0);

            process(event);

            final long end = System.nanoTime();
            long elapsed = end - start;
            serviceTime += elapsed;
            serviceOperations++;
        }
        catch (Exception ex)
        {
            // TODO: log error, do NOT remount
            ex.printStackTrace();
            System.err.println(event.getBuffer());
        }

        // Reset the event buffer in case of size larger than default.
        if (event.getBuffer().capacity() > InputBufferEvent.DEFAULT_BUFFER_SIZE)
        {
            event.resetBuffer();
        }
    }

    @Override
    public void onStart()
    {
    }

    @Override
    public void onShutdown()
    {
    }

    @Override
    public void close() throws Exception
    {
    }

    @Override
    public String toString()
    {
        return String.format("%d ops, %d ms, rate %.02g ops/s",
            serviceOperations,
            TimeUnit.NANOSECONDS.toMillis(serviceTime),
            ((double) serviceOperations / (double) serviceTime) * 1_000_000_000);
    }

    protected final long nextSequence()
    {
        return outputDisruptor.getRingBuffer().next();
    }

    protected final OutputBufferEvent getEvent(final long sequence)
    {
        return outputDisruptor.getRingBuffer().get(sequence);
    }

    protected final void publish(final long sequence)
    {
        outputDisruptor.getRingBuffer().publish(sequence);
    }

    protected abstract void process(final InputBufferEvent event) throws Exception;
}
