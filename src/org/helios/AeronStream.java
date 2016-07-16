package org.helios;

import io.aeron.Aeron;

import java.util.Objects;

public class AeronStream
{
    AeronStream(final Aeron aeron, final String channel, final int streamId)
    {
        this.aeron = Objects.requireNonNull(aeron);
        this.channel = Objects.requireNonNull(channel);
        this.streamId = streamId;
    }

    public final Aeron aeron;
    public final String channel;
    public final int streamId;
}
