package org.helios.echo;

import org.helios.Helios;
import org.helios.gateway.ServiceProxyFactory;
import org.helios.mmb.MMBPublisher;
import org.helios.mmb.MMBSubscriber;

public class EchoServiceProxyFactory implements ServiceProxyFactory<EchoServiceProxy>
{
    @Override
    public EchoServiceProxy createServiceProxy(final Helios helios, final MMBSubscriber subscriber, final MMBPublisher publisher)
    {
        return new EchoServiceProxy(subscriber, publisher);
    }
}
