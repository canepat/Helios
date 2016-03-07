package org.helios.gateway;

import com.lmax.disruptor.dsl.Disruptor;
import org.helios.core.engine.InputBufferEvent;
import org.helios.core.engine.OutputBufferEvent;
import org.helios.mmb.sbe.MessageHeaderDecoder;
import uk.co.real_logic.agrona.DirectBuffer;

public abstract class BaseServiceProxy implements ServiceProxy
{
    private final Disruptor<OutputBufferEvent> outputDisruptor;
    private final MessageHeaderDecoder headerDecoder;

    public BaseServiceProxy(final Disruptor<OutputBufferEvent> outputDisruptor)
    {
        this.outputDisruptor = outputDisruptor;

        headerDecoder = new MessageHeaderDecoder();
    }

    @Override
    public void onEvent(final InputBufferEvent event, long sequence, boolean endOfBatch) throws Exception
    {
        try
        {
            final DirectBuffer decodeBuffer = event.getBuffer();

            // Wrap the message header decoder around for decoding the message buffer.
            headerDecoder.wrap(decodeBuffer, 0);

            process(event);
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

    protected abstract void process(final InputBufferEvent event) throws Exception;
}
