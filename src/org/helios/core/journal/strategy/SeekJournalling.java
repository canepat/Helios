package org.helios.core.journal.strategy;

import org.helios.core.journal.Journalling;
import org.helios.core.journal.util.JournalAllocator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.function.Function;

public final class SeekJournalling extends AbstractJournalling<RandomAccessFile>
{
    public static Function<Path, RandomAccessFile> randomAccessFileFactory()
    {
        return (path) -> {
            try
            {
                return new RandomAccessFile(path.toString(), "rw");
            }
            catch (FileNotFoundException e)
            {
                throw new RuntimeException("Could not open file for writing: " + path.toString());
            }
        };
    }

    public SeekJournalling(final Path journalDir, final long journalSize, final int pageSize, final int journalCount)
    {
        super(journalSize, pageSize, new JournalAllocator<>(journalDir, journalCount, randomAccessFileFactory()));
    }

    @Override
    public long size() throws IOException
    {
        return currentJournal.length();
    }

    @Override
    public int read(final ByteBuffer data) throws IOException
    {
        final int dataSize = data.remaining();

        assignJournal(dataSize);

        currentJournal.seek(positionInFile);

        int bytesRead = 0;
        for (int i = data.position(); i < dataSize; i++)
        {
            data.put((byte)currentJournal.read());
            bytesRead++;
        }

        positionInFile += bytesRead;

        return bytesRead;
    }

    @Override
    public int write(final ByteBuffer data) throws IOException
    {
        final int dataSize = data.remaining();

        assignJournal(dataSize);

        currentJournal.seek(positionInFile);
        for (int i = data.position(); i < dataSize; i++)
        {
            currentJournal.write(data.get(i));
        }

        positionInFile += dataSize;

        return dataSize;
    }

    @Override
    public Journalling flush() throws IOException
    {
        currentJournal.getChannel().force(true);
        return this;
    }
}
