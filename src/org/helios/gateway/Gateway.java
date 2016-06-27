package org.helios.gateway;

import org.helios.infra.AvailableAssociationHandler;
import org.helios.infra.RateReport;
import org.helios.infra.UnavailableAssociationHandler;

public interface Gateway<T extends GatewayHandler> extends AutoCloseable
{
    Gateway<T> availableAssociationHandler(final AvailableAssociationHandler handler);

    Gateway<T> unavailableAssociationHandler(final UnavailableAssociationHandler handler);

    RateReport report();

    T handler();

    void start();
}
