package org.helios.journal.util;

import org.helios.util.Check;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;

public final class JournalAllocator<T>
{
    public static final int BLOCK_SIZE = 4096;

    private final Path journalDir;
    private final int journalCount;
    private final Function<Path, T> journalFactory;
    private final FilePreallocator filePreallocator;
    private int nextJournalNumber;

    public JournalAllocator(final Path journalDir, final int journalCount, final Function<Path, T> journalFactory)
    {
        Objects.requireNonNull(journalDir, "journalDir");
        Objects.requireNonNull(journalFactory, "journalFactory");
        Check.enforce(journalDir.toFile().exists(), "Non existent journal dir");
        Check.enforce(journalCount > 0, "Invalid non positive journal count");

        this.journalDir = journalDir;
        this.journalCount = journalCount;
        this.journalFactory = journalFactory;

        filePreallocator = new FilePreallocator(journalDir, journalCount);
    }

    public void preallocate(final long journalFileSize, final AllocationMode allocationMode) throws IOException
    {
        filePreallocator.preallocate(journalFileSize, allocationMode);
    }

    public int nextJournalNumber()
    {
        return nextJournalNumber;
    }

    public void reset()
    {
        nextJournalNumber = 0;
    }

    public T getNextJournal() throws IOException
    {
        T nextJournal = journalFactory.apply(JournalNaming.getFilePath(journalDir, nextJournalNumber));

        nextJournalNumber = nextJournalNumber + 1 < journalCount ? nextJournalNumber + 1 : 0;

        return nextJournal;
    }
}
