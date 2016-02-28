package org.helios.core.journal.util;

public class JournalNaming
{
    public static String getFileName(final int number)
    {
        return String.format("journal-%d.jnl", number);
    }
}
