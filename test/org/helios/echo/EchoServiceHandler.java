package org.helios.echo;

import com.lmax.disruptor.dsl.Disruptor;
import org.helios.core.engine.InputBufferEvent;
import org.helios.core.engine.OutputBufferEvent;
import org.helios.core.engine.BaseServiceHandler;

public class EchoServiceHandler extends BaseServiceHandler
{
    public EchoServiceHandler(final Disruptor<OutputBufferEvent> outputDisruptor)
    {
        super(outputDisruptor);
    }

    @Override
    protected void process(final InputBufferEvent inputEvent) throws Exception
    {
        final long sequence = nextSequence();
        try
        {
            OutputBufferEvent outputEvent = getEvent(sequence);
            outputEvent.getBuffer().putBytes(0, inputEvent.getBuffer(), 0, inputEvent.getBuffer().capacity());
        }
        finally
        {
            publish(sequence);
        }
    }
}
