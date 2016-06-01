package org.helios.core.snapshot;

import java.io.IOException;

public interface Snapshot
{
    void load() throws IOException;

    void save() throws IOException;
}
