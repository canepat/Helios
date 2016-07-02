package org.helios.core.journal;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.helios.core.MessageTypes;
import org.helios.core.journal.strategy.PositionalJournalling;
import org.helios.util.DirectBufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.helios.core.journal.util.FilePreallocatorTest.deleteFiles;
import static org.junit.Assert.assertTrue;

public class JournalReaderTest
{
    private static final String JOURNAL_DIR = "./runtime";
    private static final Path JOURNAL_DIR_PATH = Paths.get(JOURNAL_DIR);
    private static final int JOURNAL_PAGE_SIZE = 4 * 1024;
    private static final long JOURNAL_FILE_SIZE = 256 * JOURNAL_PAGE_SIZE;
    private static final int JOURNAL_FILE_COUNT = 1;

    private static final int MESSAGE_COUNT = 10;
    private static final int BUFFER_SIZE = 1024 + TRAILER_LENGTH;
    private static final MutableDirectBuffer MESSAGE_BUFFER =
        new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(BUFFER_SIZE));

    private Journalling journalling;

    @Before
    public void setUp()
    {
        journalling = new PositionalJournalling(
            JOURNAL_DIR_PATH, JOURNAL_FILE_SIZE, JOURNAL_PAGE_SIZE, JOURNAL_FILE_COUNT);
    }

    @Test
    public void shouldReturnZeroReadingZeroLengthJournal() throws Exception
    {
        final JournalWriter writer = new JournalWriter(journalling, true);
        writer.close();

        try (final JournalReader reader = new JournalReader(journalling))
        {
            final int messagesRead = reader.readFully((msgTypeId, buffer, index, length) -> assertTrue(false));
            assertTrue(messagesRead == 0);
        }
    }

    @Test
    public void shouldReturnZeroReadingInvalidJournal() throws Exception
    {
        journalling.write(MESSAGE_BUFFER.byteBuffer());
        journalling.close();

        try (final JournalReader reader = new JournalReader(journalling))
        {
            final int messagesRead = reader.readFully((msgTypeId, buffer, index, length) -> assertTrue(false));
            assertTrue(messagesRead == 0);
        }
    }

    @Test
    public void shouldReturnExpectedReadingOneMessageJournal() throws Exception
    {
        try(final JournalWriter writer = new JournalWriter(journalling, true))
        {
            writer.onMessage(MessageTypes.ADMINISTRATIVE_MSG_ID, MESSAGE_BUFFER, 0, BUFFER_SIZE);
        }

        try (final JournalReader reader = new JournalReader(journalling))
        {
            final int messagesRead = reader.readFully((msgTypeId, buffer, index, length) -> {
                assertTrue(msgTypeId == MessageTypes.ADMINISTRATIVE_MSG_ID);
                assertTrue(length == BUFFER_SIZE);
            });
            assertTrue(messagesRead == 1);
        }
    }

    @Test
    public void shouldReturnExpectedReadingMultipleMessageJournal() throws Exception
    {
        try(final JournalWriter writer = new JournalWriter(journalling, true))
        {
            for (int i = 0; i < MESSAGE_COUNT; i++)
            {
                MESSAGE_BUFFER.putInt(0, i);
                writer.onMessage(MessageTypes.ADMINISTRATIVE_MSG_ID, MESSAGE_BUFFER, 0, BUFFER_SIZE);
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
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenJournallingIsNull()
    {
        new JournalReader(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenMessageHandlerIsNull() throws Exception
    {
        new JournalReader(journalling).readFully(null);
    }

    @After
    public void tearDown() throws IOException
    {
        deleteFiles(JOURNAL_DIR);
    }
}
