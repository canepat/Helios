package org.helios.core.journal.util;

import org.agrona.Verify;
import org.helios.util.Check;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.file.StandardOpenOption.*;
import static org.helios.core.journal.util.JournalNaming.JOURNAL_FILE_PREFIX;

public final class FilePreallocator
{
    private static final int BLOCK_SIZE = JournalAllocator.BLOCK_SIZE;

    private final Path journalDir;
    private final int journalCount;

    public FilePreallocator(final Path journalDir, final int journalCount)
    {
        Verify.notNull(journalDir, "journalDir");
        Check.enforce(journalDir.toFile().exists(), "Non existent target dir");
        Check.enforce(journalCount > 0, "Invalid non positive journal count");

        this.journalDir = journalDir;
        this.journalCount = journalCount;
    }

    public void preallocate(final long journalSize, final AllocationMode allocationMode) throws IOException
    {
        Verify.notNull(allocationMode, "allocationMode");
        Check.enforce(journalSize > 0, "Invalid non positive journal size");

        deleteFiles();

        if (allocationMode == AllocationMode.NO_ALLOCATION)
        {
            return;
        }

        final ByteBuffer buffer = allocateDirect(BLOCK_SIZE);
        buffer.putInt(0xDEADCAFE);

        for (int i = 0; i < journalCount; i++)
        {
            try (final FileChannel channel = createFile(i))
            {
                if (allocationMode == AllocationMode.ZEROED_ALLOCATION)
                {
                    long remaining = journalSize;
                    while (remaining > 0)
                    {
                        buffer.clear();
                        remaining -= channel.write(buffer);
                    }
                }
            }
        }
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

    private FileChannel createFile(final int number) throws IOException
    {
        return FileChannel.open(JournalNaming.getFilePath(journalDir, number), CREATE, WRITE, TRUNCATE_EXISTING);
    }
}
