package org.helios.util;

import org.agrona.concurrent.SigInt;

import java.util.Objects;

public class ShutdownHelper
{
    public static void register(final Runnable shutdownHandler)
    {
        Objects.requireNonNull(shutdownHandler);

        SigInt.register(shutdownHandler);
        SigTerm.register(shutdownHandler);
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownHandler));
    }
}
