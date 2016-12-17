package org.helios.infra;

public interface Processor extends Runnable, AutoCloseable
{
    String name();

    void start();
}
