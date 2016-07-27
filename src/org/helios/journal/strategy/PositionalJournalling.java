package org.helios.journal.strategy;

import org.helios.journal.Journalling;
import org.helios.journal.util.JournalAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.function.Function;

import static java.nio.file.StandardOpenOption.*;

public final class PositionalJournalling extends AbstractJournalling<FileChannel>
{
    public static Function<Path, FileChannel> fileChannelFactory()
    {
        return (path) -> {
            try
            {
                return FileChannel.open(path, CREATE, READ, WRITE);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Could not open file for writing: " + path.toString());
            }
        };
    }

    public PositionalJournalling(final Path journalDir, final long journalSize, final int pageSize, final int journalCount)
    {
        super(journalSize, pageSize, new JournalAllocator<>(journalDir, journalCount, fileChannelFactory()));
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
    public Journalling flush() throws IOException
    {
        currentJournal.force(true);
        return this;
    }
}
