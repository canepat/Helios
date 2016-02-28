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

import uk.co.real_logic.agrona.DirectBuffer;

public interface MMBPublisher extends AutoCloseable
{
    void send(final DirectBuffer buffer, final int offset, final int length);

    void send(final DirectBuffer buffer, final int length);

    long offer(final DirectBuffer buffer, final int offset, final int length);

    long offer(final DirectBuffer buffer, final int length);
}
