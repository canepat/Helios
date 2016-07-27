package org.helios.journal.strategy;

import org.helios.journal.Journalling;

import java.nio.file.Path;

public class SeekJournallingTest extends AbstractJournallingTest
{
    @Override
    protected Journalling createJournalling(Path journalDir, long journalSize, int pageSize, int journalCount)
    {
        return new SeekJournalling(journalDir, journalSize, pageSize, journalCount);
    }
}
