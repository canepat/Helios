package org.helios.journal.util;

import org.agrona.Verify;
import org.helios.util.Check;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.*;
import static org.helios.journal.util.JournalNaming.JOURNAL_FILE_PREFIX;

public final class FilePreallocator
{
    private static final int BLOCK_SIZE = JournalAllocator.BLOCK_SIZE;

    private final Path journalDirPath;
    private final File journalDirFile;
    private final int journalCount;

    public FilePreallocator(final Path journalDirPath, final int journalCount)
    {
        Verify.notNull(journalDirPath, "journalDirPath");
        Check.enforce(journalDirPath.toFile().isDirectory(), "Journal dir path is not a directory");
        Check.enforce(journalDirPath.toFile().exists(), "Non existent journal dir path");
        Check.enforce(journalCount > 0, "Invalid non positive journal count");

        this.journalDirPath = journalDirPath;
        this.journalDirFile = journalDirPath.toFile();
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

        final ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
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
        final File[] journalFileList = journalDirFile.listFiles();
        if (journalFileList != null)
        {
            for (File journalFile : journalFileList)
            {
                if (journalFile.getName().startsWith(JOURNAL_FILE_PREFIX))
                {
                    journalFile.delete();
                }
            }
        }
    }

    private FileChannel createFile(final int number) throws IOException
    {
        return FileChannel.open(JournalNaming.getFilePath(journalDirPath, number), CREATE, WRITE, TRUNCATE_EXISTING);
    }
}
