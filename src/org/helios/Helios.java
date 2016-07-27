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
package org.helios;

import io.aeron.*;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.Verify;
import org.agrona.collections.Long2ObjectHashMap;
import org.helios.service.Service;
import org.helios.service.ServiceHandler;
import org.helios.service.ServiceHandlerFactory;
import org.helios.gateway.Gateway;
import org.helios.gateway.GatewayHandler;
import org.helios.gateway.GatewayHandlerFactory;
import org.helios.infra.RateReporter;
import org.helios.util.ProcessorHelper;

import java.util.Objects;
import java.util.function.Consumer;

public class Helios implements AutoCloseable, ErrorHandler, AvailableImageHandler, UnavailableImageHandler
{
    private final HeliosContext context;
    private final HeliosDriver driver;
    private final Aeron aeron;
    private Consumer<Throwable> errorHandler;
    private final Long2ObjectHashMap<HeliosService<?>> serviceRepository;
    private final Long2ObjectHashMap<HeliosGateway<?>> gatewayRepository;

    private RateReporter reporter;

    public Helios()
    {
        this(new HeliosContext());
    }

    public Helios(final HeliosContext context)
    {
        this(context, new HeliosDriver(context));
    }

    public Helios(final HeliosContext context, final HeliosDriver driver)
    {
        this.context = context;
        this.driver = driver;

        final Aeron.Context aeronContext = new Aeron.Context()
            .errorHandler(this).availableImageHandler(this).unavailableImageHandler(this);
        if (context.isMediaDriverEmbedded())
        {
            aeronContext.aeronDirectoryName(driver.mediaDriver().aeronDirectoryName());
        }
        aeron = Aeron.connect(aeronContext);

        errorHandler = System.err::println;

        serviceRepository = new Long2ObjectHashMap<>();
        gatewayRepository = new Long2ObjectHashMap<>();

        reporter = context.isReportingEnabled() ? new RateReporter() : null;
    }

    public void start()
    {
        gatewayRepository.values().forEach(Gateway::start);
        serviceRepository.values().forEach(Service::start);

        ProcessorHelper.start(reporter);
    }

    @Override
    public void close()
    {
        CloseHelper.quietClose(reporter);

        gatewayRepository.values().forEach(CloseHelper::quietClose);
        gatewayRepository.clear();

        serviceRepository.values().forEach(CloseHelper::quietClose);
        serviceRepository.clear();

        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(driver);
    }

    @Override
    public void onError(Throwable throwable)
    {
        errorHandler.accept(throwable);
    }

    @Override
    public void onAvailableImage(final Image image)
    {
        final long subscriptionId = image.subscription().registrationId();

        final HeliosService<?> svc = serviceRepository.get(subscriptionId);
        if (svc != null)
        {
            svc.onAssociationEstablished();
        }

        final HeliosGateway<?> gw = gatewayRepository.get(subscriptionId);
        if (gw != null)
        {
            gw.onAssociationEstablished();
        }
    }

    @Override
    public void onUnavailableImage(final Image image)
    {
        final long subscriptionId = image.subscription().registrationId();

        final HeliosService<?> svc = serviceRepository.get(subscriptionId);
        if (svc != null)
        {
            svc.onAssociationBroken();
        }

        final HeliosGateway<?> gw = gatewayRepository.get(subscriptionId);
        if (gw != null)
        {
            gw.onAssociationBroken();
        }
    }

    public Helios errorHandler(Consumer<Throwable> errorHandler)
    {
        this.errorHandler = Objects.requireNonNull(errorHandler);
        return this;
    }

    public AeronStream newStream(final String channel, final int streamId)
    {
        return new AeronStream(aeron, channel, streamId);
    }

    public <T extends ServiceHandler> Service<T> addEmbeddedService(
        final int reqStreamId, final int rspStreamId, final ServiceHandlerFactory<T> factory)
    {
        final AeronStream reqStream = new AeronStream(aeron, CommonContext.IPC_CHANNEL, reqStreamId);
        final AeronStream rspStream = new AeronStream(aeron, CommonContext.IPC_CHANNEL, rspStreamId);

        return addService(reqStream, rspStream, factory);
    }

    public <T extends ServiceHandler> Service<T> addService(final AeronStream reqStream, final AeronStream rspStream,
        final ServiceHandlerFactory<T> factory)
    {
        Objects.requireNonNull(reqStream, "reqStream");
        Objects.requireNonNull(rspStream, "rspStream");
        Objects.requireNonNull(factory, "factory");

        final HeliosService<T> svc = new HeliosService<>(context, reqStream, rspStream, factory);
        final long subscriptionId = svc.inputSubscriptionId();
        serviceRepository.put(subscriptionId, svc);

        if (reporter != null)
        {
            reporter.add(svc.report());
        }

        return svc;
    }

    public <T extends GatewayHandler> Gateway<T> addEmbeddedGateway(
        final int reqStreamId, final int rspStreamId, final GatewayHandlerFactory<T> factory)
    {
        final AeronStream reqStream = new AeronStream(aeron, CommonContext.IPC_CHANNEL, reqStreamId);
        final AeronStream rspStream = new AeronStream(aeron, CommonContext.IPC_CHANNEL, rspStreamId);

        return addGateway(reqStream, rspStream, factory);
    }

    public <T extends GatewayHandler> Gateway<T> addGateway(final AeronStream reqStream, final AeronStream rspStream,
        final GatewayHandlerFactory<T> factory)
    {
        Verify.notNull(reqStream, "reqStream");
        Verify.notNull(rspStream, "rspStream");
        Verify.notNull(factory, "factory");

        final HeliosGateway<T> gw = new HeliosGateway<>(context, reqStream, rspStream, factory);
        final long subscriptionId = gw.inputSubscriptionId();
        gatewayRepository.put(subscriptionId, gw);

        if (reporter != null)
        {
            reporter.add(gw.report());
        }

        return gw;
    }
}
