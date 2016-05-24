package org.helios.core.journal.strategy;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface JournalStrategy extends AutoCloseable
{
    void open();

    void read(final ByteBuffer data) throws IOException;

    void write(final ByteBuffer data) throws IOException;

    void reset();

    void flush() throws IOException;
}
