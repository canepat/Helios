package org.helios;

import io.aeron.Aeron;
import io.aeron.Image;
import org.helios.mmb.sbe.ComponentType;

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
    public ComponentType componentType;
    public short componentId;

    private final String key;

    @Override
    public boolean equals(Object obj)
    {
        return obj != null && obj instanceof AeronStream && ((AeronStream) obj).key.equals(key);
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
