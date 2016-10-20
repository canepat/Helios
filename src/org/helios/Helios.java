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
import org.agrona.collections.Long2ObjectHashMap;
import org.helios.gateway.Gateway;
import org.helios.gateway.GatewayHandler;
import org.helios.gateway.GatewayHandlerFactory;
import org.helios.infra.AvailableAssociationHandler;
import org.helios.infra.RateReporter;
import org.helios.infra.UnavailableAssociationHandler;
import org.helios.service.Service;
import org.helios.service.ServiceHandler;
import org.helios.service.ServiceHandlerFactory;
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
        final String sourceIdentity = image.sourceIdentity();
        final int sessionId = image.sessionId();
        System.out.println("onAvailableImage subscription.registrationId="+image.subscription().registrationId()
            +" subscription.channel="+image.subscription().channel()
            +" subscription.streamId="+image.subscription().streamId()
            +" sourceIdentity="+sourceIdentity+" sessionId="+sessionId+" correlationId="+image.correlationId());

        final HeliosService<?> svc = serviceRepository.get(subscriptionId);
        if (svc != null)
        {
            svc.onAvailableImage(image);
        }

        final HeliosGateway<?> gw = gatewayRepository.get(subscriptionId);
        if (gw != null)
        {
            gw.onAvailableImage(image);
        }
    }

    @Override
    public void onUnavailableImage(final Image image)
    {
        final long subscriptionId = image.subscription().registrationId();

        final HeliosService<?> svc = serviceRepository.get(subscriptionId);
        if (svc != null)
        {
            svc.onUnavailableImage(image);
        }

        final HeliosGateway<?> gw = gatewayRepository.get(subscriptionId);
        if (gw != null)
        {
            gw.onUnavailableImage(image);
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
        return addEmbeddedService(reqStreamId, rspStreamId, factory, null, null);
    }

    public <T extends ServiceHandler> Service<T> addEmbeddedService(
        final int reqStreamId, final int rspStreamId, final ServiceHandlerFactory<T> factory,
        final AvailableAssociationHandler availableHandler, final UnavailableAssociationHandler unavailableHandler)
    {
        final AeronStream reqStream = newStream(CommonContext.IPC_CHANNEL, reqStreamId);
        final AeronStream rspStream = newStream(CommonContext.IPC_CHANNEL, rspStreamId);

        final Service<T> svc = addService(reqStream, factory)
            .availableAssociationHandler(availableHandler)
            .unavailableAssociationHandler(unavailableHandler);

        return svc.addGateway(rspStream);
    }

    public <T extends ServiceHandler> Service<T> addService(final AeronStream reqStream, final ServiceHandlerFactory<T> factory)
    {
        Objects.requireNonNull(reqStream, "reqStream");
        Objects.requireNonNull(factory, "factory");

        final HeliosService<T> svc = new HeliosService<>(context, reqStream, factory);
        final long subscriptionId = svc.inputSubscriptionId();
        serviceRepository.put(subscriptionId, svc);
        System.out.println("HeliosService reqStream=" + reqStream + " subscriptionId="+subscriptionId);
        if (reporter != null)
        {
            reporter.add(svc.report());
        }

        return svc;
    }

    public <T extends GatewayHandler> Gateway<T> addEmbeddedGateway(
        final int reqStreamId, final int rspStreamId, final GatewayHandlerFactory<T> factory)
    {
        return addEmbeddedGateway(reqStreamId, rspStreamId, factory, null, null);
    }

    public <T extends GatewayHandler> Gateway<T> addEmbeddedGateway(
        final int reqStreamId, final int rspStreamId, final GatewayHandlerFactory<T> factory,
        final AvailableAssociationHandler availableHandler, final UnavailableAssociationHandler unavailableHandler)
    {
        final AeronStream reqStream = newStream(CommonContext.IPC_CHANNEL, reqStreamId);
        final AeronStream rspStream = newStream(CommonContext.IPC_CHANNEL, rspStreamId);

        final Gateway<T> gw = addGateway(reqStream, rspStream, factory)
            .availableAssociationHandler(availableHandler)
            .unavailableAssociationHandler(unavailableHandler);

        return gw;
    }

    public <T extends GatewayHandler> Gateway<T> addGateway(final AeronStream reqStream, final AeronStream rspStream,
        final GatewayHandlerFactory<T> factory)
    {
        Objects.requireNonNull(reqStream, "reqStream");
        Objects.requireNonNull(rspStream, "rspStream");
        Objects.requireNonNull(factory, "factory");

        final HeliosGateway<T> gw = new HeliosGateway<>(context, reqStream, rspStream, factory);
        final long subscriptionId = gw.inputSubscriptionId();
        gatewayRepository.put(subscriptionId, gw);
        System.out.println("HeliosGateway subscriptionId="+subscriptionId);
        if (reporter != null)
        {
            reporter.add(gw.report());
        }

        return gw;
    }
}
