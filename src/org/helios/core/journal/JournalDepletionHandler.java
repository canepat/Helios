package org.helios.core.journal;

@FunctionalInterface
public interface JournalDepletionHandler
{
    void onJournalDepletion(final Journalling journalling);
}
