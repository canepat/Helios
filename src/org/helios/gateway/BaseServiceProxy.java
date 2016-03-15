package org.helios.gateway;

import org.helios.mmb.MMBPublisher;
import org.helios.mmb.MMBSubscriber;
import org.helios.mmb.sbe.MessageHeaderDecoder;
import org.helios.mmb.sbe.MessageHeaderEncoder;
import uk.co.real_logic.agrona.DirectBuffer;

public abstract class BaseServiceProxy implements ServiceProxy
{
    private final MMBSubscriber subscriber;
    private final MMBPublisher publisher;

    private final MessageHeaderDecoder headerDecoder;
    private final MessageHeaderEncoder headerEncoder;

    public BaseServiceProxy(final MMBSubscriber subscriber, final MMBPublisher publisher)
    {
        this.subscriber = subscriber;
        this.publisher = publisher;

        headerDecoder = new MessageHeaderDecoder();
        headerEncoder = new MessageHeaderEncoder();
    }

    @Override
    public void close() throws Exception
    {
        subscriber.close();
        publisher.close();
    }

    @Override
    public long send(final DirectBuffer buffer, final int length)
    {
        return publisher.offer(buffer, length);
    }

    @Override
    public long receive() throws Exception
    {
        return subscriber.poll(this, 1);
    }

    @Override
    public void idle(int workCount)
    {
        subscriber.idle(workCount);
    }
}
