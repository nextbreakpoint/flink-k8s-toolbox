package com.nextbreakpoint.flink.vertex

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.RunnerOptions
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.common.ScaleJobOptions
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.common.TaskManagerId
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterContext
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.operator.core.Cache
import com.nextbreakpoint.flink.k8s.operator.core.CacheAdapter
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.action.JobDetails
import com.nextbreakpoint.flink.k8s.controller.action.JobManagerMetrics
import com.nextbreakpoint.flink.k8s.controller.action.JobMetrics
import com.nextbreakpoint.flink.k8s.controller.action.TaskManagerDetails
import com.nextbreakpoint.flink.k8s.controller.action.TaskManagerMetrics
import com.nextbreakpoint.flink.k8s.controller.action.TaskManagersList
import com.nextbreakpoint.flink.k8s.operator.OperatorRunner
import com.nextbreakpoint.flink.vertex.core.Command
import io.kubernetes.client.openapi.JSON
import io.kubernetes.client.openapi.models.V1ObjectMeta
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

        val pollingInterval: Long = config.getLong("polling_interval", 30)

        val taskTimeout: Long = config.getLong("task_timeout", 300)

        val dryRun: Boolean = config.getBoolean("dry_run", false)

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

        val cache = Cache(namespace)

        val cacheAdapter = CacheAdapter(kubeClient, cache)

        val controller = Controller(flinkOptions, flinkClient, kubeClient, dryRun)

        val registry = BackendRegistries.getNow("flink-operator")

        val runner = OperatorRunner(registry, controller, cache, RunnerOptions(pollingInterval = pollingInterval, taskTimeout = taskTimeout))

        val mainRouter = Router.router(vertx)

        mainRouter.route().handler(LoggerHandler.create(true, LoggerFormat.DEFAULT))
        mainRouter.route().handler(BodyHandler.create())
        mainRouter.route().handler(TimeoutHandler.create(120000))

        mainRouter.options("/").handler { routingContext ->
            routingContext.response().setStatusCode(204).end()
        }


        mainRouter.put("/clusters/:clusterName/start").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/start", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findClusterSelectorV2(namespace, context.pathParam("clusterName")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.put("/clusters/:clusterName/stop").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/stop", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findClusterSelectorV2(namespace, context.pathParam("clusterName")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.put("/clusters/:clusterName/scale").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/scale", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findClusterSelectorV2(namespace, context.pathParam("clusterName")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.delete("/clusters/:clusterName").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/delete", BiFunction { context, namespace ->
                json.serialize(
                    cache.findClusterSelectorV2(namespace, context.pathParam("clusterName"))
                )
            })
        }

        mainRouter.post("/clusters/:clusterName").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/cluster/create", BiFunction { context, namespace ->
                json.serialize(
                    makeV2FlinkCluster(namespace, context.pathParam("clusterName"), context.bodyAsString)
                )
            })
        }


        mainRouter.put("/clusters/:clusterName/jobs/:jobName/start").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/job/start", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findJobSelector(namespace, context.pathParam("clusterName") + "-" + context.pathParam("jobName")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.put("/clusters/:clusterName/jobs/:jobName/stop").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/job/stop", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findJobSelector(namespace, context.pathParam("clusterName") + "-" + context.pathParam("jobName")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.put("/clusters/:clusterName/jobs/:jobName/scale").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/job/scale", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findJobSelector(namespace, context.pathParam("clusterName") + "-" + context.pathParam("jobName")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.put("/clusters/:clusterName/jobs/:jobName/savepoint").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/job/savepoint/trigger", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findJobSelector(namespace, context.pathParam("clusterName") + "-" + context.pathParam("jobName")), context.bodyAsString
                    )
                )
            })
        }

        mainRouter.delete("/clusters/:clusterName/jobs/:jobName/savepoint").handler { routingContext ->
            sendMessageAndWaitForReply(routingContext, namespace, "/job/savepoint/forget", BiFunction { context, namespace ->
                json.serialize(
                    Command(
                        cache.findJobSelector(namespace, context.pathParam("clusterName") + "-" + context.pathParam("jobName")), "{}"
                    )
                )
            })
        }


        mainRouter.get("/clusters").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    cache.getClusterSelectorsV2().map { it.name }.toList()
                )
            })
        }

        mainRouter.get("/clusters/:clusterName/status").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    controller.getClusterStatus(
                        cache.findClusterSelectorV2(namespace, context.pathParam("clusterName")),
                        ClusterContext(
                            cache.getFlinkClusterV2(cache.findClusterSelectorV2(namespace, context.pathParam("clusterName")))
                        )
                    )
                )
            })
        }


        mainRouter.get("/clusters/:clusterName/jobs").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    cache.getFlinkClusterV2(cache.findClusterSelectorV2(
                        namespace, context.pathParam("clusterName"))
                    ).status.jobs.map { it.name }.toList()
                )
            })
        }


        mainRouter.get("/clusters/:clusterName/jobs/:jobName/status").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    controller.getJobStatus(
                        cache.findJobSelector(namespace, context.pathParam("clusterName") + "-" + context.pathParam("jobName")),
                        JobContext(
                            cache.getFlinkJob(cache.findJobSelector(namespace, context.pathParam("clusterName") + "-" + context.pathParam("jobName")))
                        )
                    )
                )
            })
        }

        mainRouter.get("/clusters/:clusterName/jobs/:jobName/details").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    JobDetails(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelectorV2(namespace, context.pathParam("clusterName")),
                        cache.findJobId(cache.findJobSelector(namespace, context.pathParam("clusterName") + "-" + context.pathParam("jobName")))
                    )
                )
            })
        }

        mainRouter.get("/clusters/:clusterName/jobs/:jobName/metrics").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    JobMetrics(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelectorV2(namespace, context.pathParam("clusterName")),
                        cache.findJobId(cache.findJobSelector(namespace, context.pathParam("clusterName") + "-" + context.pathParam("jobName")))
                    )
                )
            })
        }


        mainRouter.get("/clusters/:clusterName/jobmanager/metrics").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    JobManagerMetrics(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelectorV2(namespace, context.pathParam("clusterName")), null
                    )
                )
            })
        }

        mainRouter.get("/clusters/:clusterName/taskmanagers").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    TaskManagersList(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelectorV2(namespace, context.pathParam("clusterName")), null
                    )
                )
            })
        }

        mainRouter.get("/clusters/:clusterName/taskmanagers/:taskmanager/details").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    TaskManagerDetails(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelectorV2(namespace, context.pathParam("clusterName")), TaskManagerId(context.pathParam("taskmanager"))
                    )
                )
            })
        }

        mainRouter.get("/clusters/:clusterName/taskmanagers/:taskmanager/metrics").handler { routingContext ->
            processRequest(routingContext, Function { context ->
                json.serialize(
                    TaskManagerMetrics(flinkOptions, flinkClient, kubeClient).execute(
                        cache.findClusterSelectorV2(namespace, context.pathParam("clusterName")), TaskManagerId(context.pathParam("taskmanager"))
                    )
                )
            })
        }


        vertx.eventBus().consumer<String>("/cluster/start") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                {
                    json.serialize(
                        controller.requestStartCluster(
                            it.resourceSelector, json.deserialize(it.json, StartOptions::class.java), ClusterContext(cache.getFlinkClusterV2(it.resourceSelector))
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/cluster/stop") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                {
                    json.serialize(
                        controller.requestStopCluster(
                            it.resourceSelector, json.deserialize(it.json, StopOptions::class.java), ClusterContext(cache.getFlinkClusterV2(it.resourceSelector))
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/cluster/scale") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                {
                    json.serialize(
                        controller.requestScaleCluster(
                            it.resourceSelector, json.deserialize(it.json, ScaleClusterOptions::class.java)
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/cluster/delete") { message ->
            processCommandAndReplyToSender<ResourceSelector>(
                message,
                {
                    json.deserialize(it.body(), ResourceSelector::class.java)
                },
                {
                    json.serialize(controller.deleteFlinkCluster(it))
                }
            )
        }

        vertx.eventBus().consumer<String>("/cluster/create") { message ->
            processCommandAndReplyToSender<V2FlinkCluster>(
                message,
                {
                    json.deserialize(it.body(), V2FlinkCluster::class.java)
                },
                {
                    json.serialize(
                        controller.createFlinkCluster(createClusterSelector(it), it)
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/job/start") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                {
                    json.serialize(
                        controller.requestStartJob(
                            it.resourceSelector, json.deserialize(it.json, StartOptions::class.java), JobContext(cache.getFlinkJob(it.resourceSelector))
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/job/stop") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                {
                    json.serialize(
                        controller.requestStopJob(
                            it.resourceSelector, json.deserialize(it.json, StopOptions::class.java), JobContext(cache.getFlinkJob(it.resourceSelector))
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/job/scale") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                {
                    json.serialize(
                        controller.requestScaleJob(
                            it.resourceSelector, json.deserialize(it.json, ScaleJobOptions::class.java)
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/job/savepoint/trigger") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                {
                    json.serialize(
                        controller.requestTriggerSavepoint(
                            it.resourceSelector, JobContext(cache.getFlinkJob(it.resourceSelector))
                        )
                    )
                }
            )
        }

        vertx.eventBus().consumer<String>("/job/savepoint/forget") { message ->
            processCommandAndReplyToSender<Command>(
                message,
                {
                    json.deserialize(
                        it.body(), Command::class.java
                    )
                },
                {
                    json.serialize(
                        controller.requestForgetSavepoint(
                            it.resourceSelector, JobContext(cache.getFlinkJob(it.resourceSelector))
                        )
                    )
                }
            )
        }


        vertx.exceptionHandler {
            error -> logger.error("An error occurred while processing the request", error)
        }

        context.runOnContext {
            cacheAdapter.watchFlinkClustersV1(namespace)
            cacheAdapter.watchFlinkClustersV2(namespace)
            cacheAdapter.watchFlinkJobs(namespace)
            cacheAdapter.watchDeployments(namespace)
            cacheAdapter.watchServices(namespace)
            cacheAdapter.watchPods(namespace)
            cacheAdapter.watchJobs(namespace)

            thread {
                runner.run()
            }
        }

        return vertx.createHttpServer(serverOptions)
            .requestHandler(mainRouter)
            .rxListen(port)
    }

    private fun createClusterSelector(it: V2FlinkCluster): ResourceSelector {
        val namespace = it.metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = it.metadata.name ?: throw RuntimeException("Missing name")
        return ResourceSelector(namespace, name,"")
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

    private fun makeError(error: Throwable) = "{\"status\":\"FAILURE\",\"error\":\"${error.message}\"}"

    private fun makeV2FlinkCluster(namespace: String, clusterName: String, json: String): V2FlinkCluster {
        val objectMeta = V1ObjectMeta().namespace(namespace).name(clusterName)
        val flinkClusterSpec = Resource.parseV2FlinkClusterSpec(json)
        val flinkCluster = V2FlinkCluster()
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
}
