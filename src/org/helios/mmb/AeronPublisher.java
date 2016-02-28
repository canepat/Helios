/*
 * Copyright 2015 - 2016 Helios Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.helios.mmb;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

public class AeronPublisher implements MMBPublisher
{
    private final Publication publication;
    private final IdleStrategy idleStrategy;

    public AeronPublisher(final Aeron aeron, String channel, int streamId)
    {
        //new BackoffIdleStrategy(100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
        this(aeron, channel, streamId, new BusySpinIdleStrategy());
    }

    public AeronPublisher(final Aeron aeron, String channel, int streamId, final IdleStrategy idleStrategy)
    {
        this(aeron.addPublication(channel, streamId), idleStrategy);
    }

    public AeronPublisher(final Publication publication, final IdleStrategy idleStrategy)
    {
        this.publication = publication;
        this.idleStrategy = idleStrategy;
    }

    @Override
    public void send(final DirectBuffer buffer, final int offset, final int length)
    {
        long backPressured = 0;
        long notConnected = 0;

        long result;
        while ((result = publication.offer(buffer, offset, length)) < 0L)
        {
            if (result == Publication.BACK_PRESSURED)
            {
                backPressured++;
            }
            else if (result == Publication.NOT_CONNECTED)
            {
                notConnected++;
            }

            idleStrategy.idle(0);
        }

        //System.out.println("Done publishing on [" + this + "]. BPC=" + backPressureCount);
    }

    @Override
    public void send(final DirectBuffer buffer, final int length)
    {
        send(buffer, 0, length);
    }

    /*@Override
    public void send(final ByteBuffer message)
    {
        final int length = message.remaining(); // message.limit() - message.position();

        buffer.putBytes(0, message, message.position(), length);
        //System.out.println("Going publishing length=" + length);
        long backPressureCount = 0;

        long result;
        while ((result = publication.offer(buffer, 0, length)) < 0L)
        {
            if (result == Publication.BACK_PRESSURED)
            {
                //System.out.println("Offer failed due to back pressure");
                backPressureCount++;
            }
            else if (result == Publication.NOT_CONNECTED)
            {
                //System.out.println("Offer failed because publisher is not yet connected to subscriber");
            }
            else
            {
                //System.out.println("Offer failed due to unknown reason");
            }

            idleStrategy.idle(0);
        }

        //System.out.println("Done publishing on [" + this + "]. BPC=" + backPressureCount);
    }

    @Override
    public long offer(final ByteBuffer messageBuffer)
    {
        final int length = messageBuffer.remaining();

        //buffer.putBytes(0, messageBuffer, messageBuffer.position(), length);

        return publication.offer(buffer, 0, length);
    }

    @Override
    public long offer(final ByteBuffer messageBuffer, final int offset, final int length)
    {
        //buffer.putBytes(0, messageBuffer, offset, length);

        return publication.offer(buffer, 0, length);
    }*/

    @Override
    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        return publication.offer(buffer, offset, length);
    }

    @Override
    public long offer(final DirectBuffer buffer, final int length)
    {
        return publication.offer(buffer, 0, length);
    }

    public void close() throws Exception
    {
        publication.close();
    }

    @Override
    public String toString()
    {
        return "channel=" + publication.channel() + " streamId=" + publication.streamId() + " session=" +
            publication.sessionId() + " position=" + publication.position();
    }
}
