package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
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
import com.nextbreakpoint.flinkoperator.controller.core.CacheBridge
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
import kotlin.concurrent.thread

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

        val adapter = CacheAdapter(kubeClient, cache)

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


        mainRouter.put("/cluster/:name/start").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/start", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findClusterSelector(namespace, context.pathParam("name")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.put("/cluster/:name/stop").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/stop", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findClusterSelector(namespace, context.pathParam("name")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.put("/cluster/:name/scale").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/scale", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findClusterSelector(namespace, context.pathParam("name")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.put("/cluster/:name/savepoint").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/savepoint/trigger", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findClusterSelector(namespace, context.pathParam("name")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.delete("/cluster/:name/savepoint").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/savepoint/forget", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findClusterSelector(namespace, context.pathParam("name")), "{}"
                    )
                )
            })
        }

        mainRouter.delete("/cluster/:name").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/delete", BiFunction { context, namespace ->
                json.serialize(
                    cache.findClusterSelector(namespace, context.pathParam("name"))
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
                    cache.getClusterSelectors().map { it.name }.toList()
                )
            })
        }

        mainRouter.get("/cluster/:name/status").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    controller.getClusterStatus(
                        cache.findClusterSelector(namespace, context.pathParam("name")),
                        CacheBridge(
                            cache.getFlinkCluster(cache.findClusterSelector(namespace, context.pathParam("name")))
                        )
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/job/details").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    JobDetails(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelector(namespace, context.pathParam("name")), null
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/job/metrics").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    JobMetrics(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelector(namespace, context.pathParam("name")), null
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/jobmanager/metrics").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    JobManagerMetrics(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelector(namespace, context.pathParam("name")), null
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/taskmanagers").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    TaskManagersList(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelector(namespace, context.pathParam("name")), null
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/taskmanagers/:taskmanager/details").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    TaskManagerDetails(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelector(namespace, context.pathParam("name")), TaskManagerId(context.pathParam("taskmanager"))
                    )
                )
            })
        }

        mainRouter.get("/cluster/:name/taskmanagers/:taskmanager/metrics").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    TaskManagerMetrics(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelector(namespace, context.pathParam("name")), TaskManagerId(context.pathParam("taskmanager"))
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
                            it.clusterSelector, json.deserialize(it.json, StartOptions::class.java), CacheBridge(cache.getFlinkCluster(it.clusterSelector))
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
                            it.clusterSelector, json.deserialize(it.json, StopOptions::class.java), CacheBridge(cache.getFlinkCluster(it.clusterSelector))
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
                            it.clusterSelector, json.deserialize(it.json, ScaleOptions::class.java)
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/cluster/delete") { message ->
            processCommandAndReplyToSender<ClusterSelector>(
                message,
                Function {
                    json.deserialize(it.body(), ClusterSelector::class.java)
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
                        controller.createFlinkCluster(ClusterSelector(it.metadata.namespace, it.metadata.name, ""), it)
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
                            it.clusterSelector, CacheBridge(cache.getFlinkCluster(it.clusterSelector))
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
                            it.clusterSelector, CacheBridge(cache.getFlinkCluster(it.clusterSelector))
                        )
                    )
                }
            )
        }


        vertx.exceptionHandler {
            error -> logger.error("An error occurred while processing the request", error)
        }

        context.runOnContext {
            adapter.watchClusters(namespace)
            adapter.watchJobs(namespace)
            adapter.watchServices(namespace)
            adapter.watchStatefuleSets(namespace)
            adapter.watchPersistentVolumeClaims(namespace)

            startReconciliation(cache, gauges, controller)
        }

        return vertx.createHttpServer(serverOptions)
            .requestHandler(mainRouter)
            .rxListen(port)
    }

    private fun startReconciliation(cache: Cache, gauges: Map<ClusterStatus, AtomicInteger>, controller: OperationController) {
        thread {
            while (!Thread.interrupted()) {
                onUpdateMetrics(cache, gauges)
                onUpdateClusters(cache, controller)
                TimeUnit.SECONDS.sleep(Timeout.POLLING_INTERVAL)
            }
        }
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
        val clusters = cache.getFlinkClusters()

        val counters = clusters.foldRight(mutableMapOf<ClusterStatus, Int>()) { flinkCluster, counters ->
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

    private fun onUpdateClusters(cache: Cache, controller: OperationController) {
        val resorces = cache.getClusterSelectors().map {
            clusterSelector -> clusterSelector to cache.getCachedResources(clusterSelector)
        }.toList()

        resorces.forEach { pair ->
            try {
                logger.info("Reconciling ${pair.first}...")

                TaskController.create(controller, pair.first).execute(pair.second)
            } catch (e: Exception) {
                logger.warn("Can't reconcile ${pair.first}", e)
            }
        }
    }
}
