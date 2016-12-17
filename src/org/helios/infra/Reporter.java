package org.helios.infra;

@FunctionalInterface
public interface Reporter
{
    void onReport(final Report report);
}
