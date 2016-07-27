package org.helios.journal.strategy;

import org.agrona.BitUtil;
import org.helios.journal.Journalling;
import org.helios.journal.util.AllocationMode;
import org.helios.util.DirectBufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.helios.journal.util.FilePreallocatorTest.deleteFiles;
import static org.junit.Assert.assertTrue;

public abstract class AbstractJournallingTest
{
    private static final String JOURNAL_DIR = "./runtime";
    private static final Path JOURNAL_DIR_PATH = Paths.get(JOURNAL_DIR);
    private static final int JOURNAL_PAGE_SIZE = 4 * 1024;
    private static final long JOURNAL_FILE_SIZE = 256 * JOURNAL_PAGE_SIZE;
    private static final int JOURNAL_FILE_COUNT = 1;

    private static final int MESSAGE_COUNT = 10;
    private static final int BUFFER_SIZE = 1024 + TRAILER_LENGTH;
    private static final ByteBuffer MESSAGE_BUFFER = DirectBufferAllocator.allocateCacheAligned(BUFFER_SIZE);

    private Journalling journalling;

    @Before
    public void setUp() throws IOException
    {
        journalling = createJournalling(JOURNAL_DIR_PATH, JOURNAL_FILE_SIZE, JOURNAL_PAGE_SIZE, JOURNAL_FILE_COUNT);
        journalling.open(AllocationMode.ZEROED_ALLOCATION);

        assertTrue(journalling.size() == JOURNAL_FILE_SIZE);
        assertTrue(journalling.pageSize() == JOURNAL_PAGE_SIZE);
        assertTrue(journalling.position() == 0);
        assertTrue(journalling.nextJournalNumber() == 0);
    }

    @Test
    public void shouldCloseWithoutReadOrWrite() throws Exception
    {
        tearDown();

        createJournalling(JOURNAL_DIR_PATH, JOURNAL_FILE_SIZE, JOURNAL_PAGE_SIZE, JOURNAL_FILE_COUNT)
            .open(AllocationMode.ZEROED_ALLOCATION)
            .close();

        setUp();
    }

    @Test
    public void shouldUpdatePositionInRead() throws Exception
    {
        int totalReadBytes = 0;

        for (int i = 0; i < MESSAGE_COUNT; i++)
        {
            final int readBytes = journalling.read(MESSAGE_BUFFER);
            totalReadBytes += readBytes;
            MESSAGE_BUFFER.clear();

            assertTrue(readBytes == BUFFER_SIZE);
            assertTrue(journalling.position() == totalReadBytes);
        }
    }

    @Test
    public void shouldUpdatePositionInWrite() throws Exception
    {
        int totalWrittenBytes = 0;

        for (int i = 0; i < MESSAGE_COUNT; i++)
        {
            MESSAGE_BUFFER.putInt(i);
            MESSAGE_BUFFER.flip();
            final int writtenBytes = journalling.write(MESSAGE_BUFFER);
            totalWrittenBytes += writtenBytes;
            MESSAGE_BUFFER.clear();

            assertTrue(writtenBytes == BitUtil.SIZE_OF_INT);
            assertTrue(journalling.position() == totalWrittenBytes);
        }
    }

    @Test
    public void shouldCloseWithoutOpen() throws Exception
    {
        createJournalling(JOURNAL_DIR_PATH, JOURNAL_FILE_SIZE, JOURNAL_PAGE_SIZE, JOURNAL_FILE_COUNT).close();
    }

    @Test
    public void shouldTriggerSnapshotDepletionHandlerOnJournalDepletion() throws Exception
    {
        final boolean[] lastJournalReached = { false };

        journalling.depletionHandler((journalling) -> lastJournalReached[0] = true);

        int totalWrittenBytes = 0;
        while (!lastJournalReached[0])
        {
            MESSAGE_BUFFER.putInt(0);
            MESSAGE_BUFFER.flip();
            final int writtenBytes = journalling.write(MESSAGE_BUFFER);
            totalWrittenBytes += writtenBytes;
            MESSAGE_BUFFER.clear();

            assertTrue(writtenBytes == BitUtil.SIZE_OF_INT);
        }

        assertTrue(totalWrittenBytes == (JOURNAL_FILE_SIZE+BitUtil.SIZE_OF_INT));

        journalling.depletionHandler(null);
    }

    @Test
    public void shouldSupportNullSnapshotDepletionHandlerOnJournalDepletion() throws Exception
    {
        int totalWrittenBytes = 0;
        while (totalWrittenBytes < (JOURNAL_FILE_SIZE + BitUtil.SIZE_OF_INT))
        {
            MESSAGE_BUFFER.putInt(0);
            MESSAGE_BUFFER.flip();
            final int writtenBytes = journalling.write(MESSAGE_BUFFER);
            totalWrittenBytes += writtenBytes;
            MESSAGE_BUFFER.clear();

            assertTrue(writtenBytes == BitUtil.SIZE_OF_INT);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenJournalDirIsNull()
    {
        createJournalling(null, JOURNAL_FILE_SIZE, JOURNAL_PAGE_SIZE, JOURNAL_FILE_COUNT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalSizeIsZero() throws Exception
    {
        createJournalling(JOURNAL_DIR_PATH, 0, JOURNAL_PAGE_SIZE, JOURNAL_FILE_COUNT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalSizeIsNegative() throws Exception
    {
        createJournalling(JOURNAL_DIR_PATH, -1, JOURNAL_PAGE_SIZE, JOURNAL_FILE_COUNT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalPageSizeIsNotPowerOfTwo() throws Exception
    {
        createJournalling(JOURNAL_DIR_PATH, JOURNAL_FILE_SIZE, 4097, JOURNAL_FILE_COUNT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalPageSizeIsZero() throws Exception
    {
        createJournalling(JOURNAL_DIR_PATH, JOURNAL_FILE_SIZE, 0, JOURNAL_FILE_COUNT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalPageSizeIsNegative() throws Exception
    {
        createJournalling(JOURNAL_DIR_PATH, JOURNAL_FILE_SIZE, -1, JOURNAL_FILE_COUNT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalFileCountIsZero() throws Exception
    {
        createJournalling(JOURNAL_DIR_PATH, JOURNAL_FILE_SIZE, JOURNAL_PAGE_SIZE, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenJournalFileCountIsNegative() throws Exception
    {
        createJournalling(JOURNAL_DIR_PATH, JOURNAL_FILE_SIZE, JOURNAL_PAGE_SIZE, -1);
    }

    @After
    public void tearDown() throws Exception
    {
        journalling.close();

        deleteFiles(JOURNAL_DIR);
    }

    protected abstract Journalling createJournalling(final Path journalDir, final long journalSize, final int pageSize,
        final int journalCount);
}
