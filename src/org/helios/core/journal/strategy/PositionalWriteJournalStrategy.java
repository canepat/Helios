package org.helios.core.journal.strategy;

import org.helios.core.journal.util.JournalAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Function;

public final class PositionalWriteJournalStrategy extends AbstractJournalStrategy<FileChannel>
{
    public PositionalWriteJournalStrategy(final Path journalDir, final long journalSize, final int journalCount)
    {
        super(journalSize, new JournalAllocator<>(journalDir, journalCount, fileChannelFactory()));
    }

    @Override
    public long size() throws IOException
    {
        return currentJournal.size();
    }

    @Override
    public int read(final ByteBuffer data) throws IOException
    {
        assignJournal(data.remaining());

        int bytesRead = currentJournal.read(data, positionInFile);

        positionInFile += bytesRead;

        return bytesRead;
    }

    @Override
    public int write(final ByteBuffer data) throws IOException
    {
        assignJournal(data.remaining());

        int bytesWritten = currentJournal.write(data, positionInFile);

        positionInFile += bytesWritten;

        return bytesWritten;
    }

    @Override
    public void flush() throws IOException
    {
        currentJournal.force(true);
    }

    private static Function<Path, FileChannel> fileChannelFactory()
    {
        return (path) -> {
            try
            {
                return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Could not open file for writing: " + path.toString());
            }
        };
    }
}
