package org.helios.echo;

import org.helios.Helios;
import org.helios.gateway.ServiceProxy;
import org.helios.gateway.ServiceProxyFactory;
import org.helios.mmb.OutputGear;

public class EchoServiceProxyFactory implements ServiceProxyFactory
{
    @Override
    public ServiceProxy createServiceProxy(final Helios helios, final OutputGear outputGear)
    {
        return new EchoServiceProxy(outputGear.getDisruptor());
    }
}
