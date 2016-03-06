package org.helios.echo;

import org.helios.core.engine.ServiceHandler;
import org.helios.core.engine.ServiceHandlerFactory;
import org.helios.Helios;
import org.helios.mmb.OutputGear;

public class EchoServiceHandlerFactory implements ServiceHandlerFactory
{
    @Override
    public ServiceHandler createServiceHandler(final Helios helios, final OutputGear outputGear)
    {
        return new EchoServiceHandler(outputGear.getDisruptor());
    }
}
