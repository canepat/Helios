package org.helios.gateway;

import org.helios.Helios;
import org.helios.mmb.MMBPublisher;
import org.helios.mmb.MMBSubscriber;

public interface ServiceProxyFactory<T extends ServiceProxy>
{
    T createServiceProxy(final Helios helios, final MMBSubscriber subscriber, final MMBPublisher publisher);
}
