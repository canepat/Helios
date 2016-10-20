package org.helios.service;

import org.helios.infra.InputMessageProcessor;
import org.helios.infra.OutputMessageProcessor;
import org.helios.infra.RateReport;

import java.io.PrintStream;
import java.util.List;

public final class ServiceReport implements RateReport
{
    private final InputMessageProcessor requestProcessor;
    private final List<OutputMessageProcessor> responseProcessorList;

    private long lastTimeStamp;
    private long lastSuccessfulReads;
    private long lastSuccessfulWrites;
    private long lastBytesWritten;

    public ServiceReport(final InputMessageProcessor requestProcessor, final List<OutputMessageProcessor> responseProcessorList)
    {
        this.requestProcessor = requestProcessor;
        this.responseProcessorList = responseProcessorList;

        lastTimeStamp = System.currentTimeMillis();
    }

    @Override
    public void print(final PrintStream stream)
    {
        final long timeStamp = System.currentTimeMillis();
        long successfulReads = requestProcessor.successfulReads();
        long successfulWrites = responseProcessorList.get(0).handler().successfulWrites(); // FIXME: handle responseProcessorList
        long bytesWritten = responseProcessorList.get(0).handler().bytesWritten(); // FIXME: handle responseProcessorList
        long failedReads = requestProcessor.failedReads();

        final long duration = timeStamp - lastTimeStamp;
        final long successfulReadsDelta = successfulReads - lastSuccessfulReads;
        final long successfulWritesDelta = successfulWrites - lastSuccessfulWrites;
        final long bytesTransferred = bytesWritten - lastBytesWritten;

        final double failureRatio = failedReads / (double)(successfulReads + failedReads);

        stream.format("ServiceReport: T %dms IN %,d messages - OUT %,d messages - %,d bytes [read failure ratio: %f]\n",
            duration, successfulReadsDelta, successfulWritesDelta, bytesTransferred, failureRatio);

        lastTimeStamp = timeStamp;
        lastSuccessfulReads = successfulReads;
        lastSuccessfulWrites = successfulWrites;
        lastBytesWritten = bytesWritten;
    }
}
