package org.helios.core.journal.measurement;


import org.HdrHistogram.Histogram;

import java.io.PrintStream;

import static java.lang.String.format;

public enum OutputFormat
{
    LONG
        {
            @Override
            public void output(final Histogram histogram, final PrintStream printStream)
            {
                printStream.append(format("%-6s%20f%n", "mean", histogram.getMean()));
                printStream.append(format("%-6s%20d%n", "min", histogram.getMinValue()));
                printStream.append(format("%-6s%20d%n", "50.00%", histogram.getValueAtPercentile(50.0d)));
                printStream.append(format("%-6s%20d%n", "90.00%", histogram.getValueAtPercentile(90.0d)));
                printStream.append(format("%-6s%20d%n", "99.00%", histogram.getValueAtPercentile(99.0d)));
                printStream.append(format("%-6s%20d%n", "99.90%", histogram.getValueAtPercentile(99.9d)));
                printStream.append(format("%-6s%20d%n", "99.99%", histogram.getValueAtPercentile(99.99d)));
                printStream.append(format("%-6s%20d%n", "max", histogram.getMaxValue()));
                printStream.append(format("%-6s%20d%n", "count", histogram.getTotalCount()));
                printStream.append("\n");
                printStream.flush();
            }
        },
    SHORT
        {
            @Override
            public void output(final Histogram histogram, final PrintStream printStream)
            {
                printStream.append(format("%.2f,%d,%d,%d,%d,%d,%d,%d,%d%n",
                    histogram.getMean(),
                    histogram.getMinValue(),
                    histogram.getValueAtPercentile(50.0d),
                    histogram.getValueAtPercentile(90.0d),
                    histogram.getValueAtPercentile(99.0d),
                    histogram.getValueAtPercentile(99.9d),
                    histogram.getValueAtPercentile(99.99d),
                    histogram.getMaxValue(),
                    histogram.getTotalCount()));
                printStream.flush();
            }
        },
    DETAIL
        {
            @Override
            public void output(final Histogram histogram, final PrintStream printStream)
            {
                histogram.outputPercentileDistribution(printStream, 1.0d);
                printStream.flush();
            }
        };

    public abstract void output(final Histogram histogram, final PrintStream printStream);
}
