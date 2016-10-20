package org.helios.service;

import org.helios.AeronStream;
import org.helios.infra.AvailableAssociationHandler;
import org.helios.infra.RateReport;
import org.helios.infra.UnavailableAssociationHandler;

public interface Service<T extends ServiceHandler> extends AutoCloseable
{
    Service<T> addGateway(final AeronStream rspStream);

    Service<T> availableAssociationHandler(final AvailableAssociationHandler handler);

    Service<T> unavailableAssociationHandler(final UnavailableAssociationHandler handler);

    RateReport report();

    T handler();

    void start();
}
