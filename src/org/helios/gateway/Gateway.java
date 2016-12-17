package org.helios.gateway;

import org.helios.AeronStream;
import org.helios.infra.AvailableAssociationHandler;
import org.helios.infra.Report;
import org.helios.infra.UnavailableAssociationHandler;

public interface Gateway<T extends GatewayHandler> extends AutoCloseable
{
    T addEndPoint(final AeronStream reqStream, final AeronStream rspStream, final GatewayHandlerFactory<T> factory);

    Gateway<T> addEventChannel(final AeronStream eventStream);

    Gateway<T> availableAssociationHandler(final AvailableAssociationHandler handler);

    Gateway<T> unavailableAssociationHandler(final UnavailableAssociationHandler handler);

    Report report();

    void start();
}
