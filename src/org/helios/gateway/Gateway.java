package org.helios.gateway;

import org.helios.infra.RateReport;

public interface Gateway<T extends GatewayHandler> extends AutoCloseable
{
    RateReport report();

    T handler();

    void start();
}
