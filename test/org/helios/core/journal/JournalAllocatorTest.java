package org.helios.core.journal;

import org.agrona.BitUtil;
import org.helios.core.journal.strategy.PositionalJournalling;
import org.helios.core.journal.strategy.SeekJournalling;
import org.helios.core.journal.util.AllocationMode;
import org.helios.core.journal.util.JournalAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class JournalAllocatorTest
{
    private static final String JOURNAL_DIR = "./runtime";
    private static final int JOURNAL_COUNT = 2;
    private static final Function<Path, FileChannel> FILE_CHANNEL_FACTORY = PositionalJournalling.fileChannelFactory();
    private static final Function<Path, RandomAccessFile> RANDOM_ACCESS_FACTORY = SeekJournalling.randomAccessFileFactory();
    private static final long JOURNAL_FILE_SIZE = 1024*1024;

    private JournalAllocator<FileChannel> fileChannelAllocator;
    private JournalAllocator<RandomAccessFile> randomAccessFileAllocator;

    @Before
    public void setUp()
    {
        fileChannelAllocator = new JournalAllocator<>(Paths.get(JOURNAL_DIR), JOURNAL_COUNT, FILE_CHANNEL_FACTORY);
        randomAccessFileAllocator = new JournalAllocator<>(Paths.get(JOURNAL_DIR), JOURNAL_COUNT, RANDOM_ACCESS_FACTORY);
    }

    /*@Test
    public void shouldPreallocateSparse() throws IOException
    {
        shouldPreallocateSparse(fileChannelAllocator);
        shouldPreallocateSparse(randomAccessFileAllocator);
    }*/

    @Test
    public void shouldPreallocateWithZero() throws IOException
    {
        shouldPreallocateWithZero(fileChannelAllocator);
        shouldPreallocateWithZero(randomAccessFileAllocator);
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

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalSizeIsNegative() throws IOException
    {
        fileChannelAllocator.preallocate(-1, AllocationMode.ZEROED_ALLOCATION);
        randomAccessFileAllocator.preallocate(-1, AllocationMode.ZEROED_ALLOCATION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalSizeIsZero() throws IOException
    {
        fileChannelAllocator.preallocate(0, AllocationMode.ZEROED_ALLOCATION);
        randomAccessFileAllocator.preallocate(0, AllocationMode.ZEROED_ALLOCATION);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenAllocationModeIsNull() throws IOException
    {
        fileChannelAllocator.preallocate(100, null);
        randomAccessFileAllocator.preallocate(100, null);
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

    private static void shouldPreallocateSparse(final JournalAllocator<?> allocator) throws IOException
    {
        allocator.preallocate(JOURNAL_FILE_SIZE, AllocationMode.SPARSE_ALLOCATION);
        checkAllocationAndRemove(JournalAllocatorTest::checkSparseAllocation);
    }

    private static void shouldPreallocateWithZero(final JournalAllocator<?> allocator) throws IOException
    {
        allocator.preallocate(JOURNAL_FILE_SIZE, AllocationMode.ZEROED_ALLOCATION);
        checkAllocationAndRemove(JournalAllocatorTest::checkZeroedAllocation);
    }

    private static void checkAllocationAndRemove(final Consumer<Path> checkAllocationFunction) throws IOException
    {
        Files.find(Paths.get(JOURNAL_DIR), 1, (path, attr) -> path.toFile().isFile())
            .forEach(path ->  {
                try
                {
                    checkAllocationFunction.accept(path);
                    Files.delete(path);
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            });
    }

    private static void checkSparseAllocation(final Path journalPath)
    {
        checkAllocation(journalPath, (ByteBuffer readBuffer) -> {
            if (readBuffer.position() == 0)
            {
                int readInt = readBuffer.getInt();
                assertThat(readInt, is(0xDEADCAFE));
            }
        });
    }

    private static void checkZeroedAllocation(final Path journalPath)
    {
        checkAllocation(journalPath, (ByteBuffer readBuffer) -> {
            int readInt = readBuffer.getInt();
            assertThat(readInt, anyOf(is(0xDEADCAFE), is(0x00000000)));
        });
    }

    private static void checkAllocation(final Path journalPath, final Consumer<ByteBuffer> checkBufferFunction)
    {
        try
        {
            FileChannel journal = FileChannel.open(journalPath, StandardOpenOption.READ);
            assertThat(journal.size(), is(JOURNAL_FILE_SIZE));

            final ByteBuffer readBuffer = ByteBuffer.allocate(JournalAllocator.BLOCK_SIZE);

            while (journal.read(readBuffer) != -1)
            {
                readBuffer.flip();

                while (readBuffer.remaining() >= BitUtil.SIZE_OF_INT)
                {
                    checkBufferFunction.accept(readBuffer);
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
}
