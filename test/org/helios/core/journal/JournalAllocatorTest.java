package org.helios.core.journal;

import org.helios.core.journal.strategy.PositionalJournalling;
import org.helios.core.journal.util.AllocationMode;
import org.helios.core.journal.util.JournalAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

public class JournalAllocatorTest
{
    private static final String JOURNAL_DIR = "./runtime";

    private JournalAllocator<FileChannel> allocator;

    @Before
    public void setUp()
    {
        allocator = new JournalAllocator<>(Paths.get(JOURNAL_DIR), 1, PositionalJournalling.fileChannelFactory());
    }

    @Test
    public void shouldPreallocate() throws IOException
    {
        allocator.preallocate(1024*1024, AllocationMode.ZEROED_ALLOCATION);

        Files.find(Paths.get(JOURNAL_DIR), 1, (path, attr) -> path.toFile().isFile())
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

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenJournalDirIsNull()
    {
        new JournalAllocator<>(null, 1, PositionalJournalling.fileChannelFactory());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenJournalFactoryIsNull()
    {
        new JournalAllocator<>(Paths.get(JOURNAL_DIR), 1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalDirDoesNotExist()
    {
        new JournalAllocator<>(Paths.get("../foo"), 1, PositionalJournalling.fileChannelFactory());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalCountIsNegative()
    {
        new JournalAllocator<>(Paths.get(JOURNAL_DIR), -1, PositionalJournalling.fileChannelFactory());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalCountIsZero()
    {
        new JournalAllocator<>(Paths.get(JOURNAL_DIR), 0, PositionalJournalling.fileChannelFactory());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalSizeIsNegative() throws IOException
    {
        allocator.preallocate(-1, AllocationMode.ZEROED_ALLOCATION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalSizeIsZero() throws IOException
    {
        allocator.preallocate(0, AllocationMode.ZEROED_ALLOCATION);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenAllocationModeIsNull() throws IOException
    {
        allocator.preallocate(100, null);
    }

    @After
    public void tearDown() throws IOException
    {
        Files.find(Paths.get(JOURNAL_DIR), 1, (path, attr) -> path.toFile().isFile())
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
}
