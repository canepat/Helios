package org.helios.gateway;

import org.helios.AeronStream;
import org.helios.infra.AvailableAssociationHandler;
import org.helios.infra.RateReport;
import org.helios.infra.UnavailableAssociationHandler;

import java.util.List;

public interface Gateway<T extends GatewayHandler> extends AutoCloseable
{
    Gateway<T> addEndPoint(final AeronStream reqStream, final AeronStream rspStream);

    Gateway<T> addEventChannel(final AeronStream eventStream);

    Gateway<T> availableAssociationHandler(final AvailableAssociationHandler handler);

    Gateway<T> unavailableAssociationHandler(final UnavailableAssociationHandler handler);

    List<RateReport> reportList();

    T handler();

    void start();
}
