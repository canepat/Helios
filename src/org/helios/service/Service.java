package org.helios.service;

import org.helios.AeronStream;
import org.helios.infra.AvailableAssociationHandler;
import org.helios.infra.Report;
import org.helios.infra.UnavailableAssociationHandler;

public interface Service<T extends ServiceHandler> extends AutoCloseable
{
    Service<T> addEndPoint(final AeronStream rspStream);

    Service<T> addEventChannel(final AeronStream eventStream);

    Service<T> availableAssociationHandler(final AvailableAssociationHandler handler);

    Service<T> unavailableAssociationHandler(final UnavailableAssociationHandler handler);

    Report report();

    T handler();

    void start();
}
