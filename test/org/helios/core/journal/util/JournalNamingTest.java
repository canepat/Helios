package org.helios.core.journal.util;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;

public class JournalNamingTest
{
    private static final Path JOURNAL_DIR = Paths.get("../journal");

    @Test
    public void shouldFilePathStartWithJournalDir() throws IOException
    {
        final Path journalFilePath = JournalNaming.getFilePath(JOURNAL_DIR, 0);
        assertTrue(journalFilePath.startsWith(JOURNAL_DIR));
        assertTrue(journalFilePath.endsWith(JournalNaming.getFileName(0)));
    }

    @Test
    public void shouldUsePrefixInFileName() throws IOException
    {
        final String journalFileName = JournalNaming.getFileName(0);
        assertTrue(journalFileName.startsWith(JournalNaming.JOURNAL_FILE_PREFIX));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenJournalDirIsNull() throws IOException
    {
        JournalNaming.getFilePath(null, 0);
    }
}
