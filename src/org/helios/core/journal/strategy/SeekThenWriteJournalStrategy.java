package org.helios.core.journal.strategy;

import org.helios.core.journal.util.JournalAllocator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.function.Function;

public final class SeekThenWriteJournalStrategy extends AbstractJournalStrategy<RandomAccessFile>
{
    public SeekThenWriteJournalStrategy(final Path journalDir, final long journalSize, final int journalCount)
    {
        super(journalSize, new JournalAllocator<>(journalDir, journalCount, randomAccessFileFactory()));
    }

    @Override
    public void read(final ByteBuffer data) throws IOException
    {

    }

    @Override
    public void write(final ByteBuffer data) throws IOException
    {
        positionInFile += data.remaining();
        assignJournal(data.remaining());

        currentJournal.seek(positionInFile);
        currentJournal.write(data.array(), data.position(), data.remaining());
    }

    @Override
    public void flush() throws IOException
    {
        currentJournal.getChannel().force(true);
    }

    private static Function<Path, RandomAccessFile> randomAccessFileFactory()
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
}
