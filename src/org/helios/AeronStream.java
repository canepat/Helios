package org.helios;

import io.aeron.Aeron;
import io.aeron.Image;

import java.util.Objects;

public class AeronStream
{
    static AeronStream fromImage(final Aeron aeron, final Image image)
    {
        return new AeronStream(aeron, image.subscription().channel(), image.subscription().streamId());
    }

    AeronStream(final Aeron aeron, final String channel, final int streamId)
    {
        this.aeron = Objects.requireNonNull(aeron);
        this.channel = Objects.requireNonNull(channel);
        this.streamId = streamId;

        this.key = channel + "::" + streamId;
    }

    public final Aeron aeron;
    public final String channel;
    public final int streamId;

    private final String key;

    @Override
    public boolean equals(Object obj)
    {
        if (obj != null && obj instanceof AeronStream)
        {
            return ((AeronStream)obj).key.equals(key);
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return key.hashCode();
    }

    @Override
    public String toString()
    {
        return key;
    }
}
