package org.helios.echo;

import org.helios.gateway.BaseServiceProxy;
import org.helios.mmb.MMBPublisher;
import org.helios.mmb.MMBSubscriber;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;

public class EchoServiceProxy extends BaseServiceProxy
{
    private long timestamp;

    public EchoServiceProxy(final MMBSubscriber subscriber, final MMBPublisher publisher)
    {
        super(subscriber, publisher);
    }

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        timestamp = buffer.getLong(offset);
    }

    public long getTimestamp()
    {
        return timestamp;
    }
}
