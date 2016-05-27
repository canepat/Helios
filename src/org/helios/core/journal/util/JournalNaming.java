package org.helios.core.journal.util;

public class JournalNaming
{
    public static final String JOURNAL_FILE_PREFIX = "journal-";

    public static String getFileName(final int number)
    {
        return String.format("journal-%d.jnl", number);
    }
}
