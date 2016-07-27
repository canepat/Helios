package org.helios.journal.util;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class JournalNaming
{
    public static final String JOURNAL_FILE_PREFIX = "journal-";

    public static Path getFilePath(final Path journalDir, final int number) throws IOException
    {
        return Paths.get(journalDir.toString(), getFileName(number));
    }

    public static String getFileName(final int number)
    {
        return String.format("journal-%d.jnl", number);
    }
}
