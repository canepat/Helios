package org.helios.core.journal.strategy;

import org.helios.core.journal.Journalling;
import org.helios.core.journal.util.AllocationMode;
import org.helios.core.journal.util.JournalAllocator;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.helios.util.Check;

import java.io.Closeable;
import java.io.IOException;

public abstract class AbstractJournalling<T extends Closeable> implements Journalling
{
    private final long fileSize;
    private final int pageSize;
    protected final JournalAllocator<T> journalAllocator;
    protected T currentJournal;
    protected long positionInFile;

    protected AbstractJournalling(final long fileSize, final int pageSize, final JournalAllocator<T> journalAllocator)
    {
        Check.enforce(fileSize % pageSize == 0, "Journal file size must be multiple of journal page size");

        this.fileSize = fileSize;
        this.pageSize = pageSize;
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
    public int pageSize()
    {
        return pageSize;
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
    public void close() throws IOException
    {
        positionInFile = 0;
        journalAllocator.reset();

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
