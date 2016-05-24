package org.helios.core.service;

import org.helios.infra.RateReport;

public interface Service<T extends ServiceHandler> extends AutoCloseable
{
    RateReport report();

    T handler();

    void start();
}
