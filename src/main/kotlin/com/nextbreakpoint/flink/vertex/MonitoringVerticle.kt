package com.nextbreakpoint.flink.vertex

import io.kubernetes.client.openapi.JSON
import io.vertx.ext.web.handler.LoggerFormat
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.RoutingContext
import io.vertx.rxjava.ext.web.handler.BodyHandler
import io.vertx.rxjava.ext.web.handler.LoggerHandler
import io.vertx.rxjava.ext.web.handler.TimeoutHandler
import io.vertx.rxjava.micrometer.PrometheusScrapingHandler
import rx.Completable
import rx.Single
import java.util.Properties
import java.util.function.Function
import java.util.logging.Level
import java.util.logging.Logger

class MonitoringVerticle(
    private val port: Int,
    private val registryName: String
) : AbstractVerticle() {
    companion object {
        private val logger: Logger = Logger.getLogger(MonitoringVerticle::class.simpleName)
    }

    override fun rxStart(): Completable {
        val mainRouter = Router.router(vertx)

        mainRouter.route().handler(LoggerHandler.create(true, LoggerFormat.DEFAULT))
        mainRouter.route().handler(BodyHandler.create())
        mainRouter.route().handler(TimeoutHandler.create(120000))

        mainRouter.options("/").handler { routingContext ->
            routingContext.response().setStatusCode(204).end()
        }

        mainRouter.get("/version").handler { routingContext ->
            handleRequest(routingContext, Function { JSON().serialize(getVersion()) })
        }

        mainRouter.get("/metrics").handler(PrometheusScrapingHandler.create(registryName))

        return vertx.createHttpServer()
            .requestHandler(mainRouter)
            .rxListen(port)
            .toCompletable()
    }

    private fun makeError(error: Throwable) = "{\"status\":\"FAILURE\",\"error\":\"${error.message}\"}"

    private fun handleRequest(context: RoutingContext, handler: Function<RoutingContext, String>) {
        Single.just(context)
            .map {
                handler.apply(context)
            }
            .doOnSuccess {
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(it)
            }
            .doOnError {
                context.response().setStatusCode(500).end(makeError(it))
            }
            .doOnError {
                logger.log(Level.WARNING, "Can't process request", it)
            }
            .subscribe()
    }

    private fun getVersion(): Map<Any, Any> {
        javaClass.classLoader.getResourceAsStream("application.properties").use {
            val props = Properties()
            props.load(it)
            return props.toMap()
        }
    }
}
