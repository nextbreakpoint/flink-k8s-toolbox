package com.nextbreakpoint.flinkoperator.controller

import io.kubernetes.client.JSON
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.handler.LoggerFormat
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.http.HttpServer
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.RoutingContext
import io.vertx.rxjava.ext.web.handler.BodyHandler
import io.vertx.rxjava.ext.web.handler.LoggerHandler
import io.vertx.rxjava.ext.web.handler.TimeoutHandler
import io.vertx.rxjava.micrometer.PrometheusScrapingHandler
import org.apache.log4j.Logger
import rx.Completable
import rx.Single
import java.util.Properties
import java.util.function.Function

class MonitoringVerticle : AbstractVerticle() {
    companion object {
        private val logger: Logger = Logger.getLogger(MonitoringVerticle::class.simpleName)
    }

    override fun rxStart(): Completable {
        return createServer(vertx.orCreateContext.config()).toCompletable()
    }

    private fun createServer(config: JsonObject): Single<HttpServer> {
        val port: Int = config.getInteger("port") ?: 8080

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

        mainRouter.get("/metrics").handler(PrometheusScrapingHandler.create("flink-operator"))

        return vertx.createHttpServer()
            .requestHandler(mainRouter)
            .rxListen(port)
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
                logger.warn("Can't process request", it)
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
