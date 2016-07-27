package org.helios.journal;

@FunctionalInterface
public interface JournalDepletionHandler
{
    void onJournalDepletion(final Journalling journalling);
}
