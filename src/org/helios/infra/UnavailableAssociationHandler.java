package org.helios.infra;

@FunctionalInterface
public interface UnavailableAssociationHandler
{
    void onAssociationBroken();
}
