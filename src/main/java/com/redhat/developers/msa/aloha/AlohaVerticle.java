/**
 * JBoss, Home of Professional Open Source
 * Copyright 2016, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redhat.developers.msa.aloha;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.impl.client.HttpClientBuilder;
import org.jboss.jbossts.star.util.TxStatus;
import org.jboss.jbossts.star.util.TxSupport;

import com.github.kennedyoliveira.hystrix.contrib.vertx.metricsstream.EventMetricsStreamHandler;

import feign.Logger;
import feign.Logger.Level;
import feign.httpclient.ApacheHttpClient;
import feign.hystrix.HystrixFeign;
import feign.opentracing.TracingClient;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.spanmanager.DefaultSpanManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.StaticHandler;

public class AlohaVerticle extends AbstractVerticle {

    private static Tracer tracer = TracingConfiguration.tracer;

    @Override
    public void start() throws Exception {
        Router router = Router.router(vertx);
        // Health Check
        router.get("/api/health").handler(ctx -> ctx.response().end("I'm ok"));

        // Tracing
        router.route().handler(TracingConfiguration::tracingHandler)
                .failureHandler(TracingConfiguration::tracingFailureHandler);

        router.route().handler(BodyHandler.create());
        router.route().handler(CorsHandler.create("*")
            .allowedMethods(new HashSet<>(Arrays.asList(HttpMethod.values())))
            .allowedHeader("Origin, X-Requested-With, Content-Type, Accept, Authorization"));

        // Aloha EndPoint
        router.get("/api/aloha").handler(ctx -> ctx.response().end(aloha()));

        String keycloackServer = System.getenv("KEYCLOAK_AUTH_SERVER_URL");

        if (keycloackServer != null) {
            // Create a JWT Auth Provider
            JWTAuth jwt = JWTAuth.create(vertx, new JsonObject()
                .put("public-key",
                    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArfmb1i36YGxYxusjzpNxmw9a/+M40naa5RxtK826nitmWESF9XiXm6bHLWmRQyhAZluFK4RZDLhQJFZTLpC/w8HdSDETYGqnrP04jL3/pV0Mw1ReKSpzi3tIde+04xGuiQM6nuR84iRraLxtoNyIiqFmHy5pmI9hQhctfZNOVvggntnhXdt/VKuguBXqitFwGbfEgrJTeRvnTkK+rR5MsRDHA3iu2ZYaM4YNAoDbqGyoI4Jdv5Kl1LsP3qESYNeagRz6pIfDZWOoJ58p/TldVt2h70S1bzappbgs8ZbmJXg+pHWcKvNutp5y8nYw30qzU73pX6DW9JS936OB6PiU0QIDAQAB"));
            router.route("/api/aloha-secured").handler(JWTAuthHandler.create(jwt));
        }
        router.get("/api/aloha-secured").handler(ctx -> {
            User user = ctx.user();
            ctx.response().end("This is a secured resource. You're logged as " + user.principal().getString("name"));   
        });

        // Aloha Chained Endpoint
        router.get("/api/aloha-chaining").handler(ctx -> alohaChaining(ctx, (list) -> ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(Json.encode(list))));

        router.head("/api/:pid/participant").handler(ctx -> txnParticipant(ctx));
        router.put("/api/:pid/terminator").handler(ctx -> txnTerminator(ctx));


        // Hystrix Stream Endpoint
        router.get(EventMetricsStreamHandler.DEFAULT_HYSTRIX_PREFIX).handler(EventMetricsStreamHandler.createHandler());

        // Static content
        router.route("/*").handler(StaticHandler.create());

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);
        System.out.println("Service running at 0.0.0.0:8080");
    }

    private String aloha() {
        String hostname = System.getenv().getOrDefault("HOSTNAME", "unknown");
        return String.format("Aloha mai %s", hostname);
    }

    private void alohaChaining(RoutingContext context, Handler<List<String>> resultHandler) {
        String tmEnlistUri = context.request().getParam("tmEnlistUri");
        vertx.<List<String>> executeBlocking(
            // Invoke the service in a worker thread, as it's blocking.
            future -> {
                String participantUid = Integer.toString(new Random().nextInt(Integer.MAX_VALUE) + 1);
                String header = new TxSupport().makeTwoPhaseAwareParticipantLinkHeader("http://aloha:8080/api", /*volatile*/ false, participantUid, null);
                System.out.println("Enlistment uri: " + tmEnlistUri + ", header :" + header);
                String participant = new TxSupport().enlistParticipant(tmEnlistUri, header);
                System.out.println("Enlisted participant url: " + participant);

                List<String> greetings = new ArrayList<>();
                greetings.add(aloha());
                greetings.add(getNextService(context).bonjour(tmEnlistUri));
                future.complete(greetings);
            },
            ar -> {
                // Back to the event loop
                // result cannot be null, hystrix would have called the fallback.
                List<String> greetings = ar.result();
                resultHandler.handle(greetings);
            });
    }
    
    private void txnParticipant(RoutingContext ctx) {
        String pid = ctx.request().getParam("pid");
        String uri = ctx.request().absoluteURI();

        vertx.<String> executeBlocking(future ->
        {
            System.out.println("pid:" + pid + ", " + uri);
            Pattern pattern = Pattern.compile("^(.*/api/)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(uri);
            matcher.find();
            try {
                String linkHeader = new TxSupport().makeTwoPhaseAwareParticipantLinkHeader(matcher.group(1), false, pid, null);
                System.out.println("Asked to get participant terminator info - returning: " + linkHeader);
                future.complete(linkHeader);
            } catch (Throwable t) {
                t.printStackTrace();
                future.fail("error");
            }
        },
        result -> {
            ctx.response()
                .putHeader("Content-type", "text/plain");
            if (!result.failed()) ctx.response().putHeader("Link", result.result());
            ctx.response().end();
        });
    }
    
    private void txnTerminator(RoutingContext ctx) {
        String pid = ctx.request().getParam("pid");
        TxStatus status = TxSupport.toTxStatus(ctx.getBodyAsString());
        System.out.println("For pid " + pid + ", status: " + status);

        if (status.isPrepare()) {
            System.out.println("Service: preparing");
        } else if (status.isCommit()) {
            System.out.println("Service: committing");
        } else if (status.isAbort()) {
            System.out.println("Service: aborting");
        } else {
            ctx.fail(400);
        }
        
        ctx.response().end();
    }

    /**
     * This is were the "magic" happens: it creates a Feign, which is a proxy interface for remote calling a REST endpoint with
     * Hystrix fallback support.
     *
     * @return The feign pointing to the service URL and with Hystrix fallback.
     */
    private BonjourService getNextService(RoutingContext context) {
        final Span serverSpan = context.get(TracingConfiguration.ACTIVE_SPAN);
        return HystrixFeign.builder()
            // Use apache HttpClient which contains the ZipKin Interceptors
            .client(new TracingClient(new ApacheHttpClient(HttpClientBuilder.create().build()), tracer))
            // bind span to current thread
            .requestInterceptor((t) -> DefaultSpanManager.getInstance().activate(serverSpan))
            .logger(new Logger.ErrorLogger()).logLevel(Level.BASIC)
            .target(BonjourService.class, "http://bonjour:8080/",
                (String tmEnlistUri) -> "Bonjour response (fallback)");
    }

}
