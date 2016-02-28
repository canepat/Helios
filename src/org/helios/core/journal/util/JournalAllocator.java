package org.helios.core.journal.util;

import org.helios.util.Check;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Function;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.channels.FileChannel.open;

public final class JournalAllocator<T>
{
    private static final int BLOCK_SIZE = 4096;

    private final Path journalDir;
    private final int journalCount;
    private final Function<Path, T> journalFactory;
    private int journalNumber;

    public JournalAllocator(final Path journalDir, final int journalCount, final Function<Path, T> journalFactory)
    {
        Check.enforce(journalCount > 0, "Invalid non positive journal count");

        this.journalDir = journalDir;
        this.journalCount = journalCount;
        this.journalFactory = journalFactory;
    }

    public void preallocate(final long fileSize) throws IOException
    {
        Check.enforce(fileSize > 0, "Invalid non positive journal size");

        final ByteBuffer buffer = allocateDirect(BLOCK_SIZE);
        buffer.putInt(0xDEADC0DE);

        for (int i = 0; i < journalCount; i++)
        {
            try (final FileChannel channel = createFile(i))
            {
                long remaining = fileSize;
                while (remaining > 0)
                {
                    buffer.clear();
                    remaining -= channel.write(buffer);
                }
            }
        }
    }

    public void reset()
    {
        journalNumber = 0;
    }

    public T getNextJournal() throws IOException
    {
        return journalFactory.apply(getFilePath(journalNumber++));
    }

    private FileChannel createFile(final int number) throws IOException
    {
        return open(getFilePath(number), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private Path getFilePath(final int number) throws IOException
    {
        return Paths.get(journalDir.toString(), JournalNaming.getFileName(number));
    }
}
