package org.helios.mmb;

@FunctionalInterface
public interface UnavailableAssociationHandler
{
    void onAssociationBroken();
}
