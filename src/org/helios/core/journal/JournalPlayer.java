package org.helios.core.journal;

import org.helios.core.journal.strategy.JournalStrategy;

import java.nio.ByteBuffer;

public class JournalPlayer
{
    private static final int PAGE_SIZE = Integer.getInteger("helios.core.journal.page_size", 4 * 1024);

    private final JournalStrategy journalStrategy;
    private final ByteBuffer readBuffer;

    public JournalPlayer(final JournalStrategy journalStrategy)
    {
        this.journalStrategy = journalStrategy;

        readBuffer = ByteBuffer.allocate(PAGE_SIZE); // TODO: can be improved with DirectBuffer?
    }
}
