package org.helios.core.journal.util;

import org.agrona.BitUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FilePreallocatorTest
{
    private static final String JOURNAL_DIR = "./runtime";
    private static final int JOURNAL_COUNT = 2;
    private static final long JOURNAL_FILE_SIZE = 1024L * 1024L;

    private FilePreallocator preallocator;

    @Before
    public void setUp()
    {
        preallocator = new FilePreallocator(Paths.get(JOURNAL_DIR), JOURNAL_COUNT);
    }

    @Test
    public void shouldPreallocateSparse() throws IOException
    {
        preallocator.preallocate(JOURNAL_FILE_SIZE, AllocationMode.SPARSE_ALLOCATION);
        checkAllocationAndRemove(JOURNAL_DIR, JOURNAL_FILE_SIZE, FilePreallocatorTest::checkSparseAllocation);
    }

    @Test
    public void shouldPreallocateWithZero() throws IOException
    {
        preallocator.preallocate(JOURNAL_FILE_SIZE, AllocationMode.ZEROED_ALLOCATION);
        checkAllocationAndRemove(JOURNAL_DIR, JOURNAL_FILE_SIZE, FilePreallocatorTest::checkZeroedAllocation);
    }

    @Test
    public void shouldNotPreallocate() throws IOException
    {
        preallocator.preallocate(JOURNAL_FILE_SIZE, AllocationMode.NO_ALLOCATION);
        checkAllocationAndRemove(JOURNAL_DIR, JOURNAL_FILE_SIZE, FilePreallocatorTest::checkNoAllocation);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenJournalDirIsNull()
    {
        new FilePreallocator(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalDirDoesNotExist()
    {
        new FilePreallocator(Paths.get("../foo"), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalCountIsNegative()
    {
        new FilePreallocator(Paths.get(JOURNAL_DIR), -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalCountIsZero()
    {
        new FilePreallocator(Paths.get(JOURNAL_DIR), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalSizeIsNegative() throws IOException
    {
        preallocator.preallocate(-1, AllocationMode.ZEROED_ALLOCATION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalSizeIsZero() throws IOException
    {
        preallocator.preallocate(0, AllocationMode.ZEROED_ALLOCATION);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenAllocationModeIsNull() throws IOException
    {
        preallocator.preallocate(100, null);
    }

    @After
    public void tearDown() throws IOException
    {
        deleteFiles(JOURNAL_DIR);
    }

    public static void checkAllocationAndRemove(final String journalDir, final long journalSize,
        final BiConsumer<Path, Long> checkAllocation) throws IOException
    {
        Files.find(Paths.get(journalDir), 1, (path, attr) -> path.toFile().isFile())
            .forEach(path ->  {
                try
                {
                    checkAllocation.accept(path, journalSize);
                    Files.delete(path);
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            });
    }

    public static void checkSparseAllocation(final Path journalPath, final long journalSize)
    {
        assertThat(journalPath.toFile().exists(), is(true));

        try
        {
            FileChannel journal = FileChannel.open(journalPath, StandardOpenOption.READ);
            assertThat(journal.size(), is(0L));
            journal.close();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public static void checkZeroedAllocation(final Path journalPath, final long journalSize)
    {
        try
        {
            FileChannel journal = FileChannel.open(journalPath, StandardOpenOption.READ);
            assertThat(journal.size(), is(journalSize));

            final ByteBuffer readBuffer = ByteBuffer.allocate(JournalAllocator.BLOCK_SIZE);

            while (journal.read(readBuffer) != -1)
            {
                readBuffer.flip();

                while (readBuffer.remaining() >= BitUtil.SIZE_OF_INT)
                {
                    int readInt = readBuffer.getInt();
                    assertThat(readInt, anyOf(is(0xDEADCAFE), is(0x00000000)));
                }

                readBuffer.clear();
            }

            journal.close();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public static void checkNoAllocation(final Path journalPath, final long journalSize)
    {
        assertThat(journalPath.toFile().exists(), is(false));
    }

    public static void deleteFiles(final String journalDir) throws IOException
    {
        Files.find(Paths.get(journalDir), 1, (path, attr) -> path.toFile().isFile())
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
