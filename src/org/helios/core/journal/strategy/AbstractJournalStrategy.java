package org.helios.core.journal.strategy;

import org.helios.core.journal.util.JournalAllocator;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.LangUtil;

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

        try
        {
            journalAllocator.preallocate(fileSize);
        }
        catch (IOException ioe)
        {
            LangUtil.rethrowUnchecked(ioe);
        }
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
        currentJournal.close();
    }

    protected void assignJournal(final int messageSize) throws IOException
    {
        if (currentJournal == null || shouldRoll(positionInFile, messageSize))
        {
            roll();
            positionInFile = 0;
        }
    }

    private boolean shouldRoll(final long position, final long messageSize)
    {
        return position + messageSize > fileSize;
    }

    private void roll() throws IOException
    {
        CloseHelper.close(currentJournal);
        currentJournal = getNextJournal();
    }

    private T getNextJournal() throws IOException
    {
        return journalAllocator.getNextJournal();
    }
}
