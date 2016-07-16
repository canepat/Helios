package org.helios.core.journal;

import org.helios.core.journal.util.AllocationMode;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Journalling extends AutoCloseable
{
    Journalling open(final AllocationMode allocationMode);

    Journalling ensure(final int dataSize) throws IOException;

    int pageSize();

    long position();

    long size() throws IOException;

    int nextJournalNumber();

    int read(final ByteBuffer data) throws IOException;

    int write(final ByteBuffer data) throws IOException;

    Journalling flush() throws IOException;

    Journalling depletionHandler(final JournalDepletionHandler handler);
}
