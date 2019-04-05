package com.nextbreakpoint

import com.google.gson.Gson
import com.nextbreakpoint.handler.*
import com.nextbreakpoint.model.*
import io.kubernetes.client.ApiClient
import io.kubernetes.client.Configuration
import io.kubernetes.client.util.Config
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.handler.LoggerFormat
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.http.HttpServer
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.handler.BodyHandler
import io.vertx.rxjava.ext.web.handler.LoggerHandler
import io.vertx.rxjava.ext.web.handler.TimeoutHandler
import rx.Completable
import rx.Single
import java.io.File
import java.io.FileInputStream
import java.util.concurrent.TimeUnit

class FilnkSubmitServerMain : AbstractVerticle() {
    override fun rxStart(): Completable {
        return createServer(vertx.orCreateContext.config()).toCompletable()
    }

    private fun createServer(config: JsonObject): Single<HttpServer> {
        val port = config.getInteger("port")

        val kubeConfig = config.getString("kubeConfig")

        val mainRouter = Router.router(vertx)

        mainRouter.route().handler(LoggerHandler.create(true, LoggerFormat.DEFAULT))
        mainRouter.route().handler(BodyHandler.create())
        mainRouter.route().handler(TimeoutHandler.create(120000))

        Configuration.setDefaultApiClient(createKubernetesClient(kubeConfig))

        mainRouter.post("/listJobs").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(ListJobsHandler.execute(Gson().fromJson(context.bodyAsString, JobListConfig::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/submitJob").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(SubmitJobHandler.execute(Gson().fromJson(context.bodyAsString, JobSubmitConfig::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/cancelJob").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(CancelJobHandler.execute(Gson().fromJson(context.bodyAsString, JobCancelConfig::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/createCluster").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(CreateClusterHandler.execute(Gson().fromJson(context.bodyAsString, ClusterConfig::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/deleteCluster").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(DeleteClusterHandler.execute(Gson().fromJson(context.bodyAsString, ClusterDescriptor::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.options("/").handler { context ->
            context.response().setStatusCode(204).end()
        }

        return vertx.createHttpServer().requestHandler(mainRouter).rxListen(port)
    }

    private fun createKubernetesClient(kubeConfig: String?): ApiClient? {
        val client = if (kubeConfig != null) Config.fromConfig(FileInputStream(File(kubeConfig))) else Config.fromCluster()
        client.httpClient.setConnectTimeout(20000, TimeUnit.MILLISECONDS)
        client.httpClient.setWriteTimeout(30000, TimeUnit.MILLISECONDS)
        client.httpClient.setReadTimeout(30000, TimeUnit.MILLISECONDS)
        client.isDebugging = true
        return client
    }

    private fun makeError(error: Throwable) = error.message
}
