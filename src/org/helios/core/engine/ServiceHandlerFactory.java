package org.helios.core.engine;

import org.helios.mmb.Helios;
import org.helios.mmb.OutputGear;

public interface ServiceHandlerFactory
{
    BaseServiceHandler createServiceHandler(final Helios helios, final OutputGear outputGear);
}
