package org.helios.util;

import uk.co.real_logic.agrona.concurrent.SigInt;

public class ShutdownHelper
{
    public static void register(final Runnable shutdownHandler)
    {
        SigInt.register(shutdownHandler);
        SigTerm.register(shutdownHandler);
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownHandler));
    }
}
