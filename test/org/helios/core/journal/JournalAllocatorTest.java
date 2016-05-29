package org.helios.core.journal;

import org.helios.core.journal.strategy.PositionalJournalling;
import org.helios.core.journal.strategy.SeekJournalling;
import org.helios.core.journal.util.AllocationMode;
import org.helios.core.journal.util.FilePreallocatorTest;
import org.helios.core.journal.util.JournalAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.helios.core.journal.util.FilePreallocatorTest.checkAllocationAndRemove;
import static org.helios.core.journal.util.FilePreallocatorTest.deleteFiles;
import static org.junit.Assert.assertThat;

public class JournalAllocatorTest
{
    private static final String JOURNAL_DIR = "./runtime";
    private static final int JOURNAL_COUNT = 2;
    private static final Function<Path, FileChannel> FILE_CHANNEL_FACTORY = PositionalJournalling.fileChannelFactory();
    private static final Function<Path, RandomAccessFile> RANDOM_ACCESS_FACTORY = SeekJournalling.randomAccessFileFactory();
    private static final long JOURNAL_FILE_SIZE = 1024L * 1024L;

    private JournalAllocator<FileChannel> fileChannelAllocator;
    private JournalAllocator<RandomAccessFile> randomAccessFileAllocator;

    @Before
    public void setUp()
    {
        fileChannelAllocator = new JournalAllocator<>(Paths.get(JOURNAL_DIR), JOURNAL_COUNT, FILE_CHANNEL_FACTORY);
        randomAccessFileAllocator = new JournalAllocator<>(Paths.get(JOURNAL_DIR), JOURNAL_COUNT, RANDOM_ACCESS_FACTORY);
    }

    @Test
    public void shouldPreallocateSparse() throws IOException
    {
        shouldPreallocateSparse(fileChannelAllocator);
        shouldPreallocateSparse(randomAccessFileAllocator);
    }

    @Test
    public void shouldPreallocateWithZero() throws IOException
    {
        shouldPreallocateWithZero(fileChannelAllocator);
        shouldPreallocateWithZero(randomAccessFileAllocator);
    }

    @Test
    public void shouldNotPreallocate() throws IOException
    {
        shouldNotPreallocate(fileChannelAllocator);
        shouldNotPreallocate(randomAccessFileAllocator);
    }

    @Test
    public void shouldRotateNextJournalNumber() throws IOException
    {
        shouldRotateNextJournalNumber(fileChannelAllocator);
        shouldRotateNextJournalNumber(randomAccessFileAllocator);
    }

    @Test
    public void shouldResetNextJournalNumber() throws IOException
    {
        shouldResetNextJournalNumber(fileChannelAllocator);
        shouldResetNextJournalNumber(randomAccessFileAllocator);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenJournalDirIsNull()
    {
        new JournalAllocator<>(null, 1, FILE_CHANNEL_FACTORY);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenJournalFactoryIsNull()
    {
        new JournalAllocator<>(Paths.get(JOURNAL_DIR), 1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalDirDoesNotExist()
    {
        new JournalAllocator<>(Paths.get("../foo"), 1, FILE_CHANNEL_FACTORY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalCountIsNegative()
    {
        new JournalAllocator<>(Paths.get(JOURNAL_DIR), -1, FILE_CHANNEL_FACTORY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalCountIsZero()
    {
        new JournalAllocator<>(Paths.get(JOURNAL_DIR), 0, FILE_CHANNEL_FACTORY);
    }

    @After
    public void tearDown() throws IOException
    {
        deleteFiles(JOURNAL_DIR);
    }

    private static void shouldPreallocateSparse(final JournalAllocator<?> allocator) throws IOException
    {
        allocator.preallocate(JOURNAL_FILE_SIZE, AllocationMode.SPARSE_ALLOCATION);
        checkAllocationAndRemove(JOURNAL_DIR, JOURNAL_FILE_SIZE, FilePreallocatorTest::checkSparseAllocation);
    }

    private static void shouldPreallocateWithZero(final JournalAllocator<?> allocator) throws IOException
    {
        allocator.preallocate(JOURNAL_FILE_SIZE, AllocationMode.ZEROED_ALLOCATION);
        checkAllocationAndRemove(JOURNAL_DIR, JOURNAL_FILE_SIZE, FilePreallocatorTest::checkZeroedAllocation);
    }

    private static void shouldNotPreallocate(final JournalAllocator<?> allocator) throws IOException
    {
        allocator.preallocate(JOURNAL_FILE_SIZE, AllocationMode.NO_ALLOCATION);
        checkAllocationAndRemove(JOURNAL_DIR, JOURNAL_FILE_SIZE, FilePreallocatorTest::checkNoAllocation);
    }

    private static void shouldRotateNextJournalNumber(final JournalAllocator<?> allocator) throws IOException
    {
        assertThat(allocator.nextJournalNumber(), is(0));

        for (int i = 0; i < JOURNAL_COUNT; i++)
        {
            allocator.getNextJournal();

            assertThat(allocator.nextJournalNumber(), is((i + 1 < JOURNAL_COUNT) ? (i + 1) : 0));
        }

        assertThat(allocator.nextJournalNumber(), is(0));
    }

    private static void shouldResetNextJournalNumber(final JournalAllocator<?> allocator) throws IOException
    {
        allocator.reset();
        assertThat(allocator.nextJournalNumber(), is(0));
    }
}
