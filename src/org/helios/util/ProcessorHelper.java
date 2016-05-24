package org.helios.util;

import org.agrona.LangUtil;
import org.helios.infra.Processor;

public class ProcessorHelper
{
    public static void start(final Processor processor)
    {
        try
        {
            if (null != processor)
            {
                processor.start();
            }
        }
        catch (Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
