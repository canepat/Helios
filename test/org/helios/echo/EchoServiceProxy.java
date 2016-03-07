package org.helios.echo;

import com.lmax.disruptor.dsl.Disruptor;
import org.helios.core.engine.InputBufferEvent;
import org.helios.core.engine.OutputBufferEvent;
import org.helios.gateway.BaseServiceProxy;

public class EchoServiceProxy extends BaseServiceProxy
{
    public EchoServiceProxy(final Disruptor<OutputBufferEvent> outputDisruptor)
    {
        super(outputDisruptor);
    }

    @Override
    protected void process(InputBufferEvent event) throws Exception
    {

    }
}
