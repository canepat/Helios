package org.helios.core.journal.strategy;

import org.agrona.BitUtil;
import org.helios.core.journal.JournalDepletionHandler;
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
    private final JournalAllocator<T> journalAllocator;
    private JournalDepletionHandler handler;
    protected T currentJournal;
    protected long positionInFile;

    protected AbstractJournalling(final long fileSize, final int pageSize, final JournalAllocator<T> journalAllocator)
    {
        Check.enforce(fileSize > 0, "Journal file size must be positive");
        Check.enforce(BitUtil.isPowerOfTwo(pageSize), "Journal page size must be power of 2");
        Check.enforce(fileSize % pageSize == 0, "Journal file size must be multiple of journal page size");

        this.fileSize = fileSize;
        this.pageSize = pageSize;
        this.journalAllocator = journalAllocator;
    }

    @Override
    public Journalling open(final AllocationMode allocationMode)
    {
        try
        {
            journalAllocator.preallocate(fileSize, allocationMode);

            assignJournal(0);
        }
        catch (IOException ioe)
        {
            LangUtil.rethrowUnchecked(ioe);
        }

        return this;
    }

    @Override
    public Journalling ensure(int dataSize) throws IOException
    {
        assignJournal(dataSize);
        return this;
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

        CloseHelper.close(currentJournal);
        currentJournal = null;
    }

    @Override
    public Journalling depletionHandler(final JournalDepletionHandler handler)
    {
        this.handler = handler;
        return this;
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

        if (lastJournalReached() && handler != null)
        {
            handler.onJournalDepletion(this);
        }

        currentJournal = journalAllocator.getNextJournal();
    }

    private boolean lastJournalReached()
    {
        return currentJournal != null && journalAllocator.nextJournalNumber() == 0;
    }
}
