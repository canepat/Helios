package org.helios.core.snapshot;

import org.agrona.Verify;

public class SnapshotReader implements Runnable
{
    private final Snapshot snapshot;

    public SnapshotReader(final Snapshot snapshot)
    {
        Verify.notNull(snapshot, "snapshot");

        this.snapshot = snapshot;
    }

    @Override
    public void run()
    {
        // Load the data snapshot.
        snapshot.load();
    }
}
