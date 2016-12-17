package org.helios.infra;

public interface InputReport
{
    String name();

    long successfulReads();

    long failedReads();

    long bytesRead();

    long administrativeMessages();

    long applicationMessages();

    long heartbeatReceived();
}
