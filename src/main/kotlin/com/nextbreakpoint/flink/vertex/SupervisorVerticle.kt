package com.nextbreakpoint.flink.vertex

import com.nextbreakpoint.flink.common.ServerConfig
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import io.vertx.core.http.ClientAuth
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.net.JksOptions
import io.vertx.ext.web.handler.LoggerFormat
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.handler.BodyHandler
import io.vertx.rxjava.ext.web.handler.LoggerHandler
import io.vertx.rxjava.ext.web.handler.TimeoutHandler
import rx.Completable
import java.util.logging.Level
import java.util.logging.Logger

class SupervisorVerticle(
    private val namespace: String,
    private val clusterName: String,
    private val cache: Cache,
    private val controller: Controller,
    private val serverConfig: ServerConfig
) : AbstractVerticle() {
    companion object {
        private val logger: Logger = Logger.getLogger(SupervisorVerticle::class.simpleName)
    }

    override fun rxStart(): Completable {
        val serverOptions = createServerOptions(
            serverConfig.keystorePath,
            serverConfig.truststorePath,
            serverConfig.keystoreSecret,
            serverConfig.truststoreSecret
        )

        val mainRouter = Router.router(vertx)

        mainRouter.route().handler(LoggerHandler.create(true, LoggerFormat.DEFAULT))
        mainRouter.route().handler(BodyHandler.create())
        mainRouter.route().handler(TimeoutHandler.create(120000))

        mainRouter.options("/").handler { routingContext ->
            routingContext.response().setStatusCode(204).end()
        }

        vertx.exceptionHandler {
            error -> logger.log(Level.SEVERE, "An error occurred while processing the request", error)
        }

        return vertx.createHttpServer(serverOptions)
            .requestHandler(mainRouter)
            .rxListen(serverConfig.port)
            .toCompletable()
    }

    private fun createServerOptions(
        jksKeyStorePath: String?,
        jksTrustStorePath: String?,
        jksKeyStoreSecret: String?,
        jksTrustStoreSecret: String?
    ): HttpServerOptions {
        val serverOptions = HttpServerOptions()

        if (jksKeyStorePath != null && jksTrustStorePath != null) {
            logger.info("HTTPS with client authentication is enabled")

            serverOptions
                .setSsl(true)
                .setSni(false)
                .setKeyStoreOptions(JksOptions().setPath(jksKeyStorePath).setPassword(jksKeyStoreSecret))
                .setTrustStoreOptions(JksOptions().setPath(jksTrustStorePath).setPassword(jksTrustStoreSecret))
                .setClientAuth(ClientAuth.REQUIRED)
        } else {
            logger.log(Level.WARNING, "HTTPS not enabled!")
        }

        return serverOptions
    }
}
