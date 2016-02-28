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
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;

public class AeronSubscriber implements MMBSubscriber
{
    private final AtomicBoolean running;
    private final Subscription subscription;
    private final IdleStrategy idleStrategy;

    public AeronSubscriber(final AtomicBoolean running, final Aeron aeron, String channel, int streamId)
    {
        this(running, aeron.addSubscription(channel, streamId));
    }

    public AeronSubscriber(final AtomicBoolean running, final Subscription subscription)
    {
        // new BackoffIdleStrategy(100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
        this(running, subscription, new BusySpinIdleStrategy());
    }

    public AeronSubscriber(final AtomicBoolean running, final Subscription subscription, final IdleStrategy idleStrategy)
    {
        this.running = running;
        this.subscription = subscription;
        this.idleStrategy = idleStrategy;
    }

    @Override
    public void receive(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        try
        {
            while (running.get())
            {
                final int fragmentsRead = subscription.poll(fragmentHandler, fragmentLimit);
                idleStrategy.idle(fragmentsRead);
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    @Override
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        int fragmentsRead = 0;

        try
        {
            fragmentsRead = subscription.poll(fragmentHandler, fragmentLimit);
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return fragmentsRead;
    }

    public void close() throws Exception
    {
        subscription.close();
    }

    @Override
    public String toString()
    {
        return "channel=" + subscription.channel() + " streamId=" + subscription.streamId();
    }
}
