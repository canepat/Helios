package org.helios.infra;

import java.util.List;

public interface Report
{
    String name();

    List<InputReport> inputReports();

    List<OutputReport> outputReports();
}
