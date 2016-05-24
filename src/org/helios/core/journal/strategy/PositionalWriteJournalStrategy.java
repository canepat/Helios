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
    private long positionInFile;

    public PositionalWriteJournalStrategy(final Path journalDir, final long journalSize, final int journalCount)
    {
        super(journalSize, new JournalAllocator<>(journalDir, journalCount, fileChannelFactory()));
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

        currentJournal.write(data, positionInFile);
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
                return FileChannel.open(path, StandardOpenOption.WRITE);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Could not open file for writing: " + path.toString());
            }
        };
    }
}
