package org.helios.gateway;

public interface ServiceGateway extends AutoCloseable
{
    void start();

    void stop();
}
