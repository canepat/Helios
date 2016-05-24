package org.helios;

import io.aeron.Aeron;

public class AeronStream
{
    AeronStream(final Aeron aeron, final String channel, final int streamId)
    {
        this.aeron = aeron;
        this.channel = channel;
        this.streamId = streamId;
    }

    public final Aeron aeron;
    public final String channel;
    public final int streamId;
}
