package org.helios.journal;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.MessageTypes;
import org.helios.journal.strategy.PositionalJournalling;
import org.helios.util.DirectBufferAllocator;
import org.junit.*;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.helios.journal.util.FilePreallocatorTest.deleteFiles;
import static org.junit.Assert.assertTrue;

public class JournalHandlerTest
{
    private static final String JOURNAL_DIR = "./runtime";
    private static final Path JOURNAL_DIR_PATH = Paths.get(JOURNAL_DIR);
    private static final int JOURNAL_PAGE_SIZE = 4 * 1024;
    private static final long JOURNAL_FILE_SIZE = 256 * JOURNAL_PAGE_SIZE;
    private static final int JOURNAL_FILE_COUNT = 1;

    private static final int MESSAGE_COUNT = 10;
    private static final int BUFFER_SIZE = 1024;
    private static final MutableDirectBuffer MESSAGE_BUFFER =
        new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(BUFFER_SIZE));

    private final RingBuffer ringBuffer = new OneToOneRingBuffer(
        new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH)));
    private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

    private static Journalling journalling;

    @BeforeClass
    public static void setUpBeforeClass()
    {
        journalling = new PositionalJournalling(
            JOURNAL_DIR_PATH, JOURNAL_FILE_SIZE, JOURNAL_PAGE_SIZE, JOURNAL_FILE_COUNT);
    }

    @Test
    public void shouldPassMessagesToJournalWriterAndThenToRingBuffer() throws Exception
    {
        try (final JournalWriter writer = new JournalWriter(journalling, true);
             final JournalHandler handler = new JournalHandler(writer, ringBuffer, idleStrategy))
        {
            for (int i = 0; i < MESSAGE_COUNT; i++)
            {
                MESSAGE_BUFFER.putInt(0, i);
                handler.onMessage(MessageTypes.ADMINISTRATIVE_MSG_ID, MESSAGE_BUFFER, 0, BUFFER_SIZE);
            }
        }

        try (final JournalReader reader = new JournalReader(journalling))
        {
            final int messagesRead = reader.readFully(new MessageHandler() {
                int messageCount = 0;

                @Override
                public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
                {
                    assertTrue(msgTypeId == MessageTypes.ADMINISTRATIVE_MSG_ID);
                    assertTrue(length == BUFFER_SIZE);
                    assertTrue(buffer.getInt(index) == messageCount);
                    messageCount++;
                }
            });
            assertTrue(messagesRead == MESSAGE_COUNT);
        }

        final int[] numMessages = new int[1];
        while (numMessages[0] < MESSAGE_COUNT)
        {
            final int bytesRead = ringBuffer.read((msgTypeId, buffer, index, length) -> {
                assertTrue(msgTypeId == MessageTypes.ADMINISTRATIVE_MSG_ID);
                assertTrue(length == BUFFER_SIZE);
                assertTrue(buffer.getInt(index) == numMessages[0]);

                numMessages[0]++;
            });
            idleStrategy.idle(bytesRead);
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenJournalWriterIsNull()
    {
        new JournalHandler(null, ringBuffer, idleStrategy);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenRingBufferIsNull() throws Exception
    {
        try (final JournalWriter writer = new JournalWriter(journalling, true))
        {
            try
            {
                new JournalHandler(writer, null, idleStrategy);
            }
            finally
            {
                writer.depletionHandler(null);
            }
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenIdleStrategyIsNull() throws Exception
    {
        try (final JournalWriter writer = new JournalWriter(journalling, true))
        {
            try
            {
                new JournalHandler(writer, ringBuffer, null);
            }
            finally
            {
                writer.depletionHandler(null);
            }
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        journalling.close();

        deleteFiles(JOURNAL_DIR);
    }
}
