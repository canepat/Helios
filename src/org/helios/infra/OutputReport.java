package org.helios.infra;

public interface OutputReport
{
    String name();

    long successfulWrites();

    long failedWrites();

    long bytesWritten();

    long successfulBufferReads();

    long failedBufferReads();

    long heartbeatSent();
}
