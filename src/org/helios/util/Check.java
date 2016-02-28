package org.helios.util;

public abstract class Check
{
    public static void enforce(boolean expression, String message)
    {
        if (!expression)
        {
            throw new IllegalArgumentException(message);
        }
    }
}
