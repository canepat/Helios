package journal;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.helios.infra.MessageTypes;
import org.helios.journal.*;
import org.helios.journal.measurement.MeasuredJournalling;
import org.helios.journal.measurement.OutputFormat;
import org.helios.journal.strategy.PositionalJournalling;
import org.helios.util.Check;
import org.helios.util.DirectBufferAllocator;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static java.lang.System.nanoTime;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class JournalTest
{
    private static final int MESSAGE_LENGTH = JournalConfiguration.MESSAGE_LENGTH;
    private static final int NUMBER_OF_ITERATIONS = JournalConfiguration.NUMBER_OF_ITERATIONS;
    private static final int NUMBER_OF_MESSAGES = JournalConfiguration.NUMBER_OF_MESSAGES;

    private static final long MESSAGE_DISK_SIZE = MESSAGE_LENGTH + JournalRecordDescriptor.MESSAGE_FRAME_SIZE;

    public static void main(String[] args) throws Exception
    {
        final String journalDir = "./runtime";
        final int journalPageSize = 4 * 1024;
        final long journalFileSize = 256 * 1024 * journalPageSize;
        final int journalFileCount = 1;
        final boolean journalFlushing = false;

        if (journalFileSize * journalFileCount < MESSAGE_DISK_SIZE * NUMBER_OF_MESSAGES)
        {
            throw new IllegalArgumentException("Journal too short: modify test hard-coded parameters");
        }

        final Journalling journalling = new MeasuredJournalling(
            new PositionalJournalling(Paths.get(journalDir), journalFileSize, journalPageSize, journalFileCount),
            OutputFormat.LONG,
            true);

        final ByteBuffer inputBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH);
        final RingBuffer inputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(inputBuffer));

        final ByteBuffer outputBuffer = DirectBufferAllocator.allocateCacheAligned((16 * 1024) + TRAILER_LENGTH);
        final RingBuffer outputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(outputBuffer));

        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

        final JournalWriter journalWriter = new JournalWriter(journalling, journalFlushing);
        final JournalHandler journalHandler = new JournalHandler(journalWriter, outputRingBuffer, idleStrategy);
        final JournalProcessor journalProcessor = new JournalProcessor(inputRingBuffer, idleStrategy, journalHandler);

        final Producer p = new Producer(inputRingBuffer, NUMBER_OF_MESSAGES);
        final Consumer c = new Consumer(outputRingBuffer, NUMBER_OF_MESSAGES);

        journalProcessor.start();

        for (int iteration = 1; iteration <= NUMBER_OF_ITERATIONS; iteration++)
        {
            final long startTime = nanoTime();

            c.start();
            p.start();
            p.join();
            c.join();

            final long endTime = nanoTime();
            final long elapsedTime = (endTime - startTime);

            System.out.println(
                String.format("I/O latency during journal WRITE: %d iteration, %d ops, %d ns, %d ms, rate %.02g ops/s",
                    iteration, NUMBER_OF_MESSAGES, elapsedTime, TimeUnit.NANOSECONDS.toMillis(elapsedTime),
                    ((double)NUMBER_OF_MESSAGES / (double)elapsedTime) * 1_000_000_000));
        }

        journalProcessor.close();

        System.out.println();

        final JournalReader journalReader = new JournalReader(journalling);
        final JournalPlayer journalPlayer = new JournalPlayer(journalReader, outputRingBuffer, idleStrategy);

        for (int iteration = 1; iteration <= NUMBER_OF_ITERATIONS; iteration++)
        {
            final long startTime = nanoTime();

            c.start();
            journalPlayer.run();

            final int messagesReplayed = journalPlayer.messagesReplayed();
            Check.enforce(messagesReplayed == NUMBER_OF_MESSAGES,
                "Wrong messagesReplayed: expected=" + NUMBER_OF_MESSAGES + " got=" + messagesReplayed);

            c.join();

            final long endTime = nanoTime();
            final long elapsedTime = (endTime - startTime);

            System.out.println(
                String.format("I/O latency during journal READ: %d iteration, %d ops, %d ns, %d ms, rate %.02g ops/s",
                    iteration, messagesReplayed, elapsedTime, TimeUnit.NANOSECONDS.toMillis(elapsedTime),
                    ((double) messagesReplayed / (double)elapsedTime) * 1_000_000_000));
        }

        journalPlayer.close();
    }

    private static class Producer implements Runnable
    {
        private final RingBuffer inputRingBuffer;
        private final int numMessages;
        private Thread producerThread;

        Producer(final RingBuffer inputRingBuffer, final int numMessages)
        {
            this.inputRingBuffer = inputRingBuffer;
            this.numMessages = numMessages;
        }

        public void start()
        {
            producerThread = new Thread(this, "producer");
            producerThread.start();
        }

        @Override
        public void run()
        {
            final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
            final UnsafeBuffer buffer = new UnsafeBuffer(DirectBufferAllocator.allocateCacheAligned(MESSAGE_LENGTH));

            for (int messageNumber = 1; messageNumber <= numMessages; messageNumber++)
            {
                final long timestamp = System.nanoTime();

                buffer.putInt(0, messageNumber);
                buffer.putLong(4, timestamp);

                while (!inputRingBuffer.write(MessageTypes.APPLICATION_MSG_ID, buffer, 0, MESSAGE_LENGTH))
                {
                    idleStrategy.idle(0);
                }
            }
        }

        public void join() throws InterruptedException
        {
            producerThread.join();
        }
    }

    private static class Consumer implements Runnable, MessageHandler
    {
        private final RingBuffer outputRingBuffer;
        private final int numMessages;
        private Thread consumerThread;
        private int messageCount;

        Consumer(final RingBuffer outputRingBuffer, final int numMessages)
        {
            this.outputRingBuffer = outputRingBuffer;
            this.numMessages = numMessages;
        }

        public void start()
        {
            messageCount = 0;

            consumerThread = new Thread(this, "consumer");
            consumerThread.start();
        }

        @Override
        public void run()
        {
            final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

            while (messageCount < numMessages)
            {
                final int bytesRead = outputRingBuffer.read(this);
                idleStrategy.idle(bytesRead);
            }
        }

        public void join() throws InterruptedException
        {
            consumerThread.join();
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            messageCount++;

            final int messageNumber = buffer.getInt(index);
            final long timestamp = buffer.getLong(index + 4);

            Check.enforce(messageNumber == messageCount, "Unexpected messageNumber=" + messageNumber);
            Check.enforce(timestamp > 0, "Unexpected timestamp=" + timestamp);
        }
    }
}
