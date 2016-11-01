package org.helios.service;

import org.helios.AeronStream;
import org.helios.infra.AvailableAssociationHandler;
import org.helios.infra.RateReport;
import org.helios.infra.UnavailableAssociationHandler;

import java.util.List;

public interface Service<T extends ServiceHandler> extends AutoCloseable
{
    Service<T> addEndPoint(final AeronStream reqStream, final AeronStream rspStream);

    Service<T> addEventChannel(final AeronStream eventStream);

    Service<T> availableAssociationHandler(final AvailableAssociationHandler handler);

    Service<T> unavailableAssociationHandler(final UnavailableAssociationHandler handler);

    List<RateReport> reportList();

    T handler();

    void start();
}
