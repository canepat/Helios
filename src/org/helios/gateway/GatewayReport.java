package org.helios.gateway;

import org.helios.infra.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class GatewayReport implements Report
{
    private final List<InputReport> inputReportList;
    private final List<OutputReport> outputReportList;

    public GatewayReport()
    {
        inputReportList = new ArrayList<>();
        outputReportList = new ArrayList<>();
    }

    public void addRequestProcessor(final OutputMessageProcessor requestProcessor)
    {
        Objects.requireNonNull(requestProcessor, "requestProcessor");

        outputReportList.add(requestProcessor);
    }

    public void addResponseProcessor(final InputMessageProcessor responseProcessor)
    {
        Objects.requireNonNull(responseProcessor, "responseProcessor");

        inputReportList.add(responseProcessor);
    }

    @Override
    public String name()
    {
        return "GatewayReport";
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
