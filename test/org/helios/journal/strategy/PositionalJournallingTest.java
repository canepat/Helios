package org.helios.journal.strategy;

import org.helios.journal.Journalling;

import java.nio.file.Path;

public class PositionalJournallingTest extends AbstractJournallingTest
{
    @Override
    protected Journalling createJournalling(Path journalDir, long journalSize, int pageSize, int journalCount)
    {
        return new PositionalJournalling(journalDir, journalSize, pageSize, journalCount);
    }
}
