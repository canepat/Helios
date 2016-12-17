package org.helios.service;

import org.helios.infra.*;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ServiceReport implements Report
{
    private final List<InputReport> inputReportList;
    private final List<OutputReport> outputReportList;

    public ServiceReport(final InputMessageProcessor requestProcessor)
    {
        Objects.requireNonNull(requestProcessor, "requestProcessor");

        inputReportList = new ArrayList<>();
        outputReportList = new ArrayList<>();

        inputReportList.add(requestProcessor);
    }

    public void addResponseProcessor(final OutputMessageProcessor responseProcessor)
    {
        Objects.requireNonNull(responseProcessor, "responseProcessor");

        outputReportList.add(responseProcessor);
    }

    @Override
    public String name()
    {
        return "ServiceReport";
    }

    @Override
    public List<InputReport> inputReports()
    {
        return inputReportList;
    }

    @Override
    public List<OutputReport> outputReports()
    {
        return outputReportList;
    }
}
