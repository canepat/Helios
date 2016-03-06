package org.helios.core.engine;

import org.helios.Helios;
import org.helios.mmb.OutputGear;

public interface ServiceHandlerFactory
{
    ServiceHandler createServiceHandler(final Helios helios, final OutputGear outputGear);
}
