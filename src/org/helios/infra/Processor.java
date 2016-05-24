package org.helios.infra;

public interface Processor extends Runnable, AutoCloseable
{
    void start();
}
