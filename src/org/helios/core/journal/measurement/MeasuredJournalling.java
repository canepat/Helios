package org.helios.core.journal.measurement;

import org.HdrHistogram.Histogram;
import org.helios.core.journal.Journalling;
import org.helios.core.journal.util.AllocationMode;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public final class MeasuredJournalling implements Journalling
{
    private static final long HIGHEST_TRACKABLE_VALUE = TimeUnit.SECONDS.toNanos(1L);
    private static final int NUM_SIGNIFICANT_VALUE_DIGITS = 4;

    private final Journalling delegate;
    private final OutputFormat outputFormat;
    private final Histogram readHistogram = new Histogram(HIGHEST_TRACKABLE_VALUE, NUM_SIGNIFICANT_VALUE_DIGITS);
    private final Histogram writeHistogram = new Histogram(HIGHEST_TRACKABLE_VALUE, NUM_SIGNIFICANT_VALUE_DIGITS);
    private boolean recording;

    public MeasuredJournalling(final Journalling delegate, final OutputFormat outputFormat, final boolean recording)
    {
        this.delegate = delegate;
        this.outputFormat = outputFormat;
        this.recording = recording;
    }

    @Override
    public void open(AllocationMode allocationMode)
    {
        delegate.open(allocationMode);
    }

    @Override
    public void ensure(int dataSize) throws IOException
    {
        delegate.ensure(dataSize);
    }

    @Override
    public int pageSize()
    {
        return delegate.pageSize();
    }

    @Override
    public long position()
    {
        return delegate.position();
    }

    @Override
    public int nextJournalNumber()
    {
        return delegate.nextJournalNumber();
    }

    @Override
    public long size() throws IOException
    {
        return delegate.size();
    }

    @Override
    public int read(final ByteBuffer data) throws IOException
    {
        final long startTime = System.nanoTime();
        final int bytesRead = delegate.read(data);
        final long duration = System.nanoTime() - startTime;

        readHistogram.recordValue(Math.min(HIGHEST_TRACKABLE_VALUE, duration));

        return bytesRead;
    }

    @Override
    public int write(final ByteBuffer data) throws IOException
    {
        final long startTime = System.nanoTime();
        final int bytesWritten = delegate.write(data);
        final long duration = System.nanoTime() - startTime;

        writeHistogram.recordValue(Math.min(HIGHEST_TRACKABLE_VALUE, duration));

        return bytesWritten;
    }

    @Override
    public void flush() throws IOException
    {
        delegate.flush();
    }

    @Override
    public void close() throws Exception
    {
        delegate.close();

        if (recording)
        {
            final PrintStream printStream = new PrintStream(System.out);
            if (writeHistogram.getTotalCount() > 0)
            {
                printStream.append(String.format("== %s ==%n", "Histogram of journal WRITE latency in nanoseconds"));
                outputFormat.output(writeHistogram, printStream);
            }
            if (readHistogram.getTotalCount() > 0)
            {
                printStream.append(String.format("== %s ==%n", "Histogram of journal READ latency in nanoseconds"));
                outputFormat.output(readHistogram, printStream);
            }
        }

        readHistogram.reset();
        writeHistogram.reset();
    }
}
