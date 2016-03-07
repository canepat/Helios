package org.helios.gateway;

import org.helios.Helios;
import org.helios.mmb.OutputGear;

public interface ServiceProxyFactory
{
    ServiceProxy createServiceProxy(final Helios helios, final OutputGear outputGear);
}
