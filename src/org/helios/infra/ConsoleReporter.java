package org.helios.infra;

import java.io.PrintStream;

public final class ConsoleReporter implements Reporter
{
    private final PrintStream stream = System.out;
    private long lastTimestamp = System.nanoTime();

    @Override
    public void onReport(final Report report)
    {
        final long currentTimestamp = System.nanoTime();
        final long duration = currentTimestamp - lastTimestamp;
        lastTimestamp = currentTimestamp;

        stream.format("%s: T %dms ", report.name(), duration);

        for (final InputReport inputReport : report.inputReports())
        {
            final long successfulReads = inputReport.successfulReads();
            final long failedReads = inputReport.failedReads();
            final long administrativeMessages = inputReport.administrativeMessages();
            final long heartbeatReceived = inputReport.heartbeatReceived();
            final long applicationMessages = inputReport.applicationMessages();
            final long bytesRead = inputReport.bytesRead();

            final double failureRatio = failedReads / (double)(successfulReads + failedReads);

            stream.format("IN %s: %,d messages (%,d ADMIN %,d HBT %,d APP) - %,d failed - %,d bytes [read failure ratio: %f]\n",
                inputReport.name(), successfulReads, administrativeMessages, heartbeatReceived, applicationMessages,
                failedReads, bytesRead, failureRatio);
        }

        for (final OutputReport outputReport : report.outputReports())
        {
            final long successfulWrites = outputReport.successfulWrites();
            final long failedWrites = outputReport.failedWrites();
            final long heartbeatSent = outputReport.heartbeatSent();
            final long bytesWritten = outputReport.bytesWritten();
            final long successfulBufferReads = outputReport.successfulBufferReads();
            final long failedBufferReads = outputReport.failedBufferReads();

            final double failureRatio = failedWrites / (double)(successfulWrites + failedWrites);

            stream.format("OUT %s: %,d messages (%,d HBT) - %,d failed - %,d bytes [write failure ratio: %f] {buffer %,d reads %,d failed}\n",
                outputReport.name(), successfulWrites, heartbeatSent, failedWrites, bytesWritten, failureRatio,
                successfulBufferReads, failedBufferReads);
        }
    }
}
