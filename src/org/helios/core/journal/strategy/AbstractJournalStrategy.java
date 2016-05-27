package org.helios.core.journal.strategy;

import org.helios.core.journal.util.AllocationMode;
import org.helios.core.journal.util.JournalAllocator;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;

import java.io.Closeable;
import java.io.IOException;

public abstract class AbstractJournalStrategy<T extends Closeable> implements JournalStrategy
{
    private final long fileSize;
    protected final JournalAllocator<T> journalAllocator;
    protected T currentJournal;
    protected long positionInFile;

    protected AbstractJournalStrategy(final long fileSize, final JournalAllocator<T> journalAllocator)
    {
        this.fileSize = fileSize;
        this.journalAllocator = journalAllocator;
    }

    @Override
    public void open(final AllocationMode allocationMode)
    {
        try
        {
            journalAllocator.preallocate(fileSize, allocationMode);
        }
        catch (IOException ioe)
        {
            LangUtil.rethrowUnchecked(ioe);
        }
    }

    @Override
    public void ensure(int dataSize) throws IOException
    {
        assignJournal(dataSize);
    }

    @Override
    public long position()
    {
        return positionInFile;
    }

    @Override
    public int nextJournalNumber()
    {
        return journalAllocator.nextJournalNumber();
    }

    @Override
    public void reset()
    {
        positionInFile = 0;
        journalAllocator.reset();
    }

    @Override
    public void close() throws IOException
    {
        reset();

        currentJournal.close();
        currentJournal = null;
    }

    protected void assignJournal(final int dataSize) throws IOException
    {
        if (currentJournal == null || shouldRoll(dataSize))
        {
            roll();
            positionInFile = 0;
        }
    }

    private boolean shouldRoll(final long dataSize)
    {
        return positionInFile + dataSize > fileSize;
    }

    private void roll() throws IOException
    {
        CloseHelper.close(currentJournal);
        currentJournal = journalAllocator.getNextJournal();
    }
}
