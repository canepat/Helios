package org.helios.gateway;

import org.helios.Helios;
import org.helios.mmb.InputGear;

public interface ServiceEventHandlerFactory
{
    ServiceEventHandler createServiceEventHandler(final Helios helios, final InputGear inputGear);
}
