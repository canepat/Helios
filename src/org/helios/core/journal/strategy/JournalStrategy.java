package org.helios.core.journal.strategy;

import org.helios.core.journal.util.AllocationMode;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface JournalStrategy extends AutoCloseable
{
    void open(final AllocationMode allocationMode);

    void ensure(final int dataSize) throws IOException;

    long position();

    long size() throws IOException;

    int nextJournalNumber();

    int read(final ByteBuffer data) throws IOException;

    int write(final ByteBuffer data) throws IOException;

    void reset();

    void flush() throws IOException;
}
