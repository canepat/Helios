package org.helios.core.archive;

public interface ArchiveBatchHandler<E> extends AutoCloseable
{
    void onBatch(final E[] batch, int offset, int length);
}
