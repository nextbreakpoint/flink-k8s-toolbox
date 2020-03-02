package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.CacheAdapter
import com.nextbreakpoint.flinkoperator.controller.core.Command
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskController
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import com.nextbreakpoint.flinkoperator.controller.operation.JobDetails
import com.nextbreakpoint.flinkoperator.controller.operation.JobManagerMetrics
import com.nextbreakpoint.flinkoperator.controller.operation.JobMetrics
import com.nextbreakpoint.flinkoperator.controller.operation.TaskManagerDetails
import com.nextbreakpoint.flinkoperator.controller.operation.TaskManagerMetrics
import com.nextbreakpoint.flinkoperator.controller.operation.TaskManagersList
import io.kubernetes.client.JSON
import io.kubernetes.client.models.V1ObjectMeta
import io.micrometer.core.instrument.ImmutableTag
import io.micrometer.core.instrument.MeterRegistry
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.http.ClientAuth
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.JsonObject
import io.vertx.core.net.JksOptions
import io.vertx.ext.web.handler.LoggerFormat
import io.vertx.micrometer.backends.BackendRegistries
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.WorkerExecutor
import io.vertx.rxjava.core.eventbus.Message
import io.vertx.rxjava.core.http.HttpServer
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.RoutingContext
import io.vertx.rxjava.ext.web.handler.BodyHandler
import io.vertx.rxjava.ext.web.handler.LoggerHandler
import io.vertx.rxjava.ext.web.handler.TimeoutHandler
import org.apache.log4j.Logger
import rx.Completable
import rx.Single
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiFunction
import java.util.function.Function

class OperatorVerticle : AbstractVerticle() {
    companion object {
        private val logger: Logger = Logger.getLogger(OperatorVerticle::class.simpleName)
    }

    override fun rxStart(): Completable {
        return createServer(vertx.orCreateContext.config()).toCompletable()
    }

    private fun createServer(config: JsonObject): Single<HttpServer> {
        val port: Int = config.getInteger("port") ?: 4444

        val flinkHostname: String? = config.getString("flink_hostname") ?: null

        val portForward: Int? = config.getInteger("port_forward") ?: null

        val useNodePort: Boolean = config.getBoolean("use_node_port", false)

        val namespace: String = config.getString("namespace") ?: throw RuntimeException("Namespace required")

        val jksKeyStorePath = config.getString("server_keystore_path")

        val jksKeyStoreSecret = config.getString("server_keystore_secret")

        val jksTrustStorePath = config.getString("server_truststore_path")

        val jksTrustStoreSecret = config.getString("server_truststore_secret")

        val serverOptions = createServerOptions(
            jksKeyStorePath, jksTrustStorePath, jksKeyStoreSecret, jksTrustStoreSecret
        )

        val flinkOptions = FlinkOptions(
            hostname = flinkHostname,
            portForward = portForward,
            useNodePort = useNodePort
        )

        val kubeClient = KubeClient

        val flinkClient = FlinkClient

        val json = JSON()

        val cache = Cache()

        val watch = WatchAdapter(json, kubeClient, cache)

        val controller = OperationController(flinkOptions, flinkClient, kubeClient)

        val mainRouter = Router.router(vertx)

        val registry = BackendRegistries.getNow("flink-operator")

        val gauges = registerMetrics(registry, namespace)

        mainRouter.route().handler(LoggerHandler.create(true, LoggerFormat.DEFAULT))
        mainRouter.route().handler(BodyHandler.create())
        mainRouter.route().handler(TimeoutHandler.create(120000))

        mainRouter.options("/").handler { routingContext ->
            routingContext.response().setStatusCode(204).end()
        }


        val worker = vertx.createSharedWorkerExecutor("reconcile", 20, 60, TimeUnit.SECONDS)

        mainRouter.put("/cluster/:name/start").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/start", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.getClusterId(namespace, context.pathParam("name")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.put("/cluster/:name/stop").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/stop", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.getClusterId(namespace, context.pathParam("name")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.put("/cluster/:name/scale").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/scale", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.getClusterId(namespace, context.pathParam("name")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.put("/cluster/:name/savepoint").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/savepoint/trigger", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.getClusterId(namespace, context.pathParam("name")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.delete("/cluster/:name/savepoint").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/savepoint/forget", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.getClusterId(namespace, context.pathParam("name")), "{}"
                    )
                )
            })
        }

        mainRouter.delete("/cluster/:name").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/delete", BiFunction { context, namespace ->
                json.serialize(
                    cache.getClusterId(namespace, context.pathParam("name"))
                )
            })
        }

        mainRouter.post("/cluster/:name").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/create", BiFunction { context, namespace ->
                json.serialize(
                    makeV1FlinkCluster(context, namespace)
                )
            })
        }


        mainRouter.get("/clusters").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    cache.getCachedClusters().map { it.name }.toList()
                )
            })
        }

        mainRouter.get("/cluster/:name/status").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    controller.getClusterStatus(
                        cache.getClusterId(namespace, context.pathParam("name")),
                        CacheAdapter(
                            cache.getFlinkCluster(cache.getClusterId(namespace, context.pathParam("name")))
                        )
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/job/details").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    JobDetails(flinkOptions, flinkClient, kubeClient).execute(
                        cache.getClusterId(namespace, context.pathParam("name")), null
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/job/metrics").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    JobMetrics(flinkOptions, flinkClient, kubeClient).execute(
                        cache.getClusterId(namespace, context.pathParam("name")), null
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/jobmanager/metrics").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    JobManagerMetrics(flinkOptions, flinkClient, kubeClient).execute(
                        cache.getClusterId(namespace, context.pathParam("name")), null
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/taskmanagers").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    TaskManagersList(flinkOptions, flinkClient, kubeClient).execute(
                        cache.getClusterId(namespace, context.pathParam("name")), null
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/taskmanagers/:taskmanager/details").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    TaskManagerDetails(flinkOptions, flinkClient, kubeClient).execute(
                        cache.getClusterId(namespace, context.pathParam("name")), TaskManagerId(context.pathParam("taskmanager"))
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/taskmanagers/:taskmanager/metrics").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    TaskManagerMetrics(flinkOptions, flinkClient, kubeClient).execute(
                        cache.getClusterId(namespace, context.pathParam("name")), TaskManagerId(context.pathParam("taskmanager"))
                    )
                )
            })
        }


        vertx.eventBus().consumer<String>("/cluster/start") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                Function {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                Function {
                    json.serialize(
                        controller.requestStartCluster(
                            it.clusterId, json.deserialize(it.json, StartOptions::class.java), CacheAdapter(cache.getFlinkCluster(it.clusterId))
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/cluster/stop") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                Function {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                Function {
                    json.serialize(
                        controller.requestStopCluster(
                            it.clusterId, json.deserialize(it.json, StopOptions::class.java), CacheAdapter(cache.getFlinkCluster(it.clusterId))
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/cluster/scale") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                Function {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                Function {
                    json.serialize(
                        controller.requestScaleCluster(
                            it.clusterId, json.deserialize(it.json, ScaleOptions::class.java)
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/cluster/delete") { message ->
            processCommandAndReplyToSender<ClusterId>(
                message,
                Function {
                    json.deserialize(it.body(), ClusterId::class.java)
                },
                Function {
                    json.serialize(controller.deleteFlinkCluster(it))
                }
            )
        }

        vertx.eventBus().consumer<String>("/cluster/create") { message ->
            processCommandAndReplyToSender<V1FlinkCluster>(
                message,
                Function {
                    json.deserialize(it.body(), V1FlinkCluster::class.java)
                },
                Function {
                    json.serialize(
                        controller.createFlinkCluster(ClusterId(it.metadata.namespace, it.metadata.name, ""), it)
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/cluster/savepoint/trigger") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                Function {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                Function {
                    json.serialize(
                        controller.createSavepoint(
                            it.clusterId, CacheAdapter(cache.getFlinkCluster(it.clusterId))
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/cluster/savepoint/forget") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                Function {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                Function {
                    json.serialize(
                        controller.forgetSavepoint(
                            it.clusterId, CacheAdapter(cache.getFlinkCluster(it.clusterId))
                        )
                    )
                }
            )
        }


        vertx.exceptionHandler {
            error -> logger.error("An error occurred while processing the request", error)
        }

        context.runOnContext {
            watch.watchClusters(namespace)
        }

        vertx.setPeriodic(Timeout.POLLING_INTERVAL * 1000) {
            onUpdateMetrics(cache, gauges)
            onUpdateClusters(cache, worker, controller)
        }

        return vertx.createHttpServer(serverOptions)
            .requestHandler(mainRouter)
            .rxListen(port)
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
            logger.warn("HTTPS not enabled!")
        }

        return serverOptions
    }

    private fun registerMetrics(registry: MeterRegistry, namespace: String): Map<ClusterStatus, AtomicInteger> {
        return ClusterStatus.values().map {
            it to registry.gauge(
                "flink_operator.clusters_status.${it.name.toLowerCase()}",
                listOf(ImmutableTag("namespace", namespace)),
                AtomicInteger(0)
            )
        }.toMap()
    }

    private fun onUpdateMetrics(cache: Cache, gauges: Map<ClusterStatus, AtomicInteger>) {
        val counters = cache.getFlinkClusters()
            .foldRight(mutableMapOf<ClusterStatus, Int>()) { flinkCluster, counters ->
                val status = Status.getClusterStatus(flinkCluster)
                counters.compute(status) { _, value ->
                    if (value != null) value + 1 else 1
                }
                counters
            }

        ClusterStatus.values().forEach {
            gauges[it]?.set(counters[it] ?: 0)
        }
    }

    private fun makeError(error: Throwable) = "{\"status\":\"FAILURE\",\"error\":\"${error.message}\"}"

    private fun makeV1FlinkCluster(context: RoutingContext, namespace: String): V1FlinkCluster {
        val objectMeta = V1ObjectMeta().namespace(namespace).name(context.pathParam("name"))
        val flinkClusterSpec = ClusterResource.parseV1FlinkClusterSpec(context.bodyAsString)
        val flinkCluster = V1FlinkCluster()
        flinkCluster.metadata = objectMeta
        flinkCluster.spec = flinkClusterSpec
        return flinkCluster
    }

    private fun processRequest(context: RoutingContext, handler: Function<RoutingContext, String>) {
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

    private fun sendMessageAndWaitForReply(
        context: RoutingContext,
        namespace: String,
        address: String,
        handler: BiFunction<RoutingContext, String, String>
    ) {
        Single.just(context)
            .map {
                handler.apply(context, namespace)
            }
            .flatMap {
                context.vertx().eventBus().rxRequest<String>(address, it, DeliveryOptions().setSendTimeout(30000))
            }
            .doOnSuccess {
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(it.body())
            }
            .doOnError {
                context.response().setStatusCode(500).end(makeError(it))
            }
            .doOnError {
                logger.warn("Can't process request", it)
            }
            .subscribe()
    }

    private fun <T> processCommandAndReplyToSender(
        message: Message<String>,
        converter: Function<Message<String>, T>,
        handler: Function<T, String>
    ) {
        Single.just(message)
            .map {
                converter.apply(it)
            }
            .map {
                handler.apply(it)
            }
            .doOnError {
                logger.error("Can't process command [address=${message.address()}]", it)
            }
            .onErrorReturn {
                makeError(it)
            }
            .doOnSuccess {
                message.reply(it)
            }
            .doOnError {
                logger.error("Can't send response [address=${message.address()}]", it)
            }
            .subscribe()
    }

//    private fun <T> processCommand(
//        message: Message<String>,
//        converter: Function<Message<String>, T>,
//        handler: Function<T, Void?>
//    ) {
//        Single.just(message)
//            .map {
//                converter.apply(it)
//            }
//            .map {
//                handler.apply(it)
//            }
//            .doOnError {
//                logger.error("Can't process command [address=${message.address()}]", it)
//            }
//            .subscribe()
//    }
//
//    private fun <T> processMessage(
//        message: Message<String>,
//        converter: Function<Message<String>, T>,
//        handler: Consumer<T>
//    ) {
//        Single.just(message)
//            .map {
//                converter.apply(it)
//            }
//            .map {
//                handler.accept(it)
//            }
//            .doOnError {
//                logger.error("Can't process message [address=${message.address()}]", it)
//            }
//            .subscribe()
//    }

    private fun onUpdateClusters(cache: Cache, worker: WorkerExecutor, controller: OperationController) {
        cache.getCachedClusters().map {
            clusterId -> clusterId to cache.getFlinkCluster(clusterId)
        }.forEach { pair ->
            worker.rxExecuteBlocking<Void?> { promise ->
                try {
                    TaskController(controller, pair.first).execute(pair.second)

                    promise.complete()
                } catch (e: Exception) {
                    promise.fail(e)
                }
            }.doOnError {
                logger.warn("Can't update cluster", it)
            }.subscribe()
        }
    }
}
