package org.helios.echo;

import org.helios.core.engine.BaseServiceHandler;
import org.helios.core.engine.ServiceHandlerFactory;
import org.helios.mmb.Helios;
import org.helios.mmb.OutputGear;

public class EchoServiceHandlerFactory implements ServiceHandlerFactory
{
    @Override
    public BaseServiceHandler createServiceHandler(final Helios helios, final OutputGear outputGear)
    {
        return new EchoServiceHandler(outputGear.getDisruptor());
    }
}
