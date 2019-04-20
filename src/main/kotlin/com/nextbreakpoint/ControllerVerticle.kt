package com.nextbreakpoint

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils.createKubernetesClient
import com.nextbreakpoint.handler.*
import com.nextbreakpoint.model.*
import io.kubernetes.client.Configuration
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

class ControllerVerticle : AbstractVerticle() {
    override fun rxStart(): Completable {
        return createServer(vertx.orCreateContext.config()).toCompletable()
    }

    private fun makeError(error: Throwable) = error.message

    private fun createServer(config: JsonObject): Single<HttpServer> {
        val port: Int = config.getInteger("port") ?: 4444

        val portForward: Int? = config.getInteger("portForward") ?: null

        val kubeConfig: String? = config.getString("kubeConfig") ?: null

        val mainRouter = Router.router(vertx)

        mainRouter.route().handler(LoggerHandler.create(true, LoggerFormat.DEFAULT))
        mainRouter.route().handler(BodyHandler.create())
        mainRouter.route().handler(TimeoutHandler.create(120000))

        Configuration.setDefaultApiClient(createKubernetesClient(kubeConfig))

        mainRouter.post("/jobs/list").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(JobsListHandler.execute(portForward, kubeConfig != null, Gson().fromJson(context.bodyAsString, JobListConfig::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/job/run").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(JobRunHandler.execute(Gson().fromJson(context.bodyAsString, RunJobConfig::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/job/cancel").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(JobCancelHandler.execute(portForward, kubeConfig != null, Gson().fromJson(context.bodyAsString, JobCancelConfig::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/job/details").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(JobDetailsHandler.execute(portForward, kubeConfig != null, Gson().fromJson(context.bodyAsString, JobDescriptor::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/job/metrics").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(JobMetricsHandler.execute(portForward, kubeConfig != null, Gson().fromJson(context.bodyAsString, JobDescriptor::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/jobmanager/metrics").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(JobManagerMetricsHandler.execute(portForward, kubeConfig != null, Gson().fromJson(context.bodyAsString, ClusterDescriptor::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/taskmanagers/list").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(TaskManagersListHandler.execute(portForward, kubeConfig != null, Gson().fromJson(context.bodyAsString, ClusterDescriptor::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/taskmanager/details").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(TaskManagerDetailsHandler.execute(portForward, kubeConfig != null, Gson().fromJson(context.bodyAsString, TaskManagerDescriptor::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/taskmanager/metrics").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(TaskManagerMetricsHandler.execute(portForward, kubeConfig != null, Gson().fromJson(context.bodyAsString, TaskManagerDescriptor::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/cluster/create").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(ClusterCreateHandler.execute(Gson().fromJson(context.bodyAsString, ClusterConfig::class.java)))
            }.subscribe({ output ->
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
            }, { error ->
                context.response().setStatusCode(500).end(makeError(error))
            })
        }

        mainRouter.post("/cluster/delete").handler { context ->
            vertx.rxExecuteBlocking<String> { future ->
                future.complete(ClusterDeleteHandler.execute(Gson().fromJson(context.bodyAsString, ClusterDescriptor::class.java)))
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
}
