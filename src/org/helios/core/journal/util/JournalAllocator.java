package org.helios.core.journal.util;

import org.agrona.Verify;
import org.helios.util.Check;
import org.helios.util.DirectBufferAllocator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import static java.nio.file.StandardOpenOption.*;
import static org.helios.core.journal.util.JournalNaming.JOURNAL_FILE_PREFIX;

public final class JournalAllocator<T>
{
    public static final int BLOCK_SIZE = 4096;

    private final Path journalDir;
    private final int journalCount;
    private final Function<Path, T> journalFactory;
    private int nextJournalNumber;

    public JournalAllocator(final Path journalDir, final int journalCount, final Function<Path, T> journalFactory)
    {
        Verify.notNull(journalDir, "journalDir");
        Verify.notNull(journalFactory, "journalFactory");
        Check.enforce(journalDir.toFile().exists(), "Non existent journal dir");
        Check.enforce(journalCount > 0, "Invalid non positive journal count");

        this.journalDir = journalDir;
        this.journalCount = journalCount;
        this.journalFactory = journalFactory;
    }

    public void preallocate(final long fileSize, final AllocationMode allocationMode) throws IOException
    {
        Verify.notNull(allocationMode, "allocationMode");
        Check.enforce(fileSize > 0, "Invalid non positive journal size");

        deleteFiles();

        if (allocationMode == AllocationMode.NO_ALLOCATION)
        {
            return;
        }

        final ByteBuffer buffer = DirectBufferAllocator.allocateCacheAligned(BLOCK_SIZE);
        buffer.putInt(0xDEADCAFE);

        for (int i = 0; i < journalCount; i++)
        {
            try (final FileChannel channel = createFile(i))
            {
                if (allocationMode == AllocationMode.ZEROED_ALLOCATION)
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
    }

    public int nextJournalNumber()
    {
        return nextJournalNumber;
    }

    public void reset()
    {
        nextJournalNumber = 0;
    }

    public T getNextJournal() throws IOException
    {
        T nextJournal = journalFactory.apply(getFilePath(nextJournalNumber));

        nextJournalNumber = nextJournalNumber + 1 < journalCount ? nextJournalNumber + 1 : 0;

        return nextJournal;
    }

    private FileChannel createFile(final int number) throws IOException
    {
        return FileChannel.open(getFilePath(number), CREATE, WRITE, TRUNCATE_EXISTING);
    }

    private void deleteFiles() throws IOException
    {
        Files.find(journalDir, 1, (path, attr) -> path.toFile().getName().startsWith(JOURNAL_FILE_PREFIX))
            .forEach(path ->  {
                try
                {
                    Files.delete(path);
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            });
    }

    private Path getFilePath(final int number) throws IOException
    {
        return Paths.get(journalDir.toString(), JournalNaming.getFileName(number));
    }
}
