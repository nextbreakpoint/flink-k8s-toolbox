package com.nextbreakpoint.flink.vertex

import com.nextbreakpoint.flink.common.ClusterSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.JobSelector
import com.nextbreakpoint.flink.common.RunnerOptions
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.common.ScaleJobOptions
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.common.TaskManagerId
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.controller.core.ClusterContext
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.operator.OperatorRunner
import com.nextbreakpoint.flink.k8s.operator.core.Cache
import com.nextbreakpoint.flink.k8s.operator.core.CacheAdapter
import com.nextbreakpoint.flink.vertex.core.ClusterCommand
import com.nextbreakpoint.flink.vertex.core.JobCommand
import io.kubernetes.client.openapi.JSON
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
        val namespace: String = config.getString("namespace") ?: throw RuntimeException("Namespace required")

        val port: Int = config.getInteger("port") ?: 4444
        val flinkHostname: String? = config.getString("flink_hostname") ?: null
        val portForward: Int? = config.getInteger("port_forward") ?: null
        val useNodePort: Boolean = config.getBoolean("use_node_port", false)
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
            handleClusterCommand(routingContext, namespace, json, "/cluster/start") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/stop").handler { routingContext ->
            handleClusterCommand(routingContext, namespace, json, "/cluster/stop") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/scale").handler { routingContext ->
            handleClusterCommand(routingContext, namespace, json, "/cluster/scale") { context -> context.bodyAsString }
        }

        mainRouter.delete("/clusters/:clusterName").handler { routingContext ->
            handleClusterCommand(routingContext, namespace, json, "/cluster/delete") { context -> "{}" }
        }

        mainRouter.post("/clusters/:clusterName").handler { routingContext ->
            handleClusterCommand(routingContext, namespace, json, "/cluster/create") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName").handler { routingContext ->
            handleClusterCommand(routingContext, namespace, json, "/cluster/update") { context -> context.bodyAsString }
        }


        mainRouter.put("/clusters/:clusterName/jobs/:jobName/start").handler { routingContext ->
            handleJobCommand(routingContext, namespace, json, "/job/start") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/jobs/:jobName/stop").handler { routingContext ->
            handleJobCommand(routingContext, namespace, json, "/job/stop") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/jobs/:jobName/scale").handler { routingContext ->
            handleJobCommand(routingContext, namespace, json, "/job/scale") { context -> context.bodyAsString }
        }

        mainRouter.delete("/clusters/:clusterName/jobs/:jobName").handler { routingContext ->
            handleJobCommand(routingContext, namespace, json, "/job/delete") { context -> "{}" }
        }

        mainRouter.post("/clusters/:clusterName/jobs/:jobName").handler { routingContext ->
            handleJobCommand(routingContext, namespace, json, "/job/create") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/jobs/:jobName").handler { routingContext ->
            handleJobCommand(routingContext, namespace, json, "/job/update") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/jobs/:jobName/savepoint").handler { routingContext ->
            handleJobCommand(routingContext, namespace, json, "/job/savepoint/trigger") { context -> context.bodyAsString }
        }

        mainRouter.delete("/clusters/:clusterName/jobs/:jobName/savepoint").handler { routingContext ->
            handleJobCommand(routingContext, namespace, json, "/job/savepoint/forget") { context -> "{}" }
        }


        mainRouter.get("/clusters").handler { routingContext ->
            handleListClusters(routingContext, json, cache)
        }

        mainRouter.get("/clusters/:clusterName/status").handler { routingContext ->
            handleGetClusterStatus(routingContext, json, controller, namespace, cache)
        }

        mainRouter.get("/clusters/:clusterName/jobs").handler { routingContext ->
            handleListJobs(routingContext, json, cache)
        }

        mainRouter.get("/clusters/:clusterName/jobs/:jobName/status").handler { routingContext ->
            handleGetJobStatus(routingContext, json, controller, namespace, cache)
        }

        mainRouter.get("/clusters/:clusterName/jobs/:jobName/details").handler { routingContext ->
            handleGetJobDetails(routingContext, json, controller, namespace, cache)
        }

        mainRouter.get("/clusters/:clusterName/jobs/:jobName/metrics").handler { routingContext ->
            handleGetJobMetrics(routingContext, json, controller, namespace, cache)
        }

        mainRouter.get("/clusters/:clusterName/jobmanager/metrics").handler { routingContext ->
            handleGetJobManagerMetrics(routingContext, json, controller, namespace)
        }

        mainRouter.get("/clusters/:clusterName/taskmanagers").handler { routingContext ->
            handleListTaskManagers(routingContext, json, controller, namespace)
        }

        mainRouter.get("/clusters/:clusterName/taskmanagers/:taskmanager/details").handler { routingContext ->
            handleGetTaskManagerDetails(routingContext, json, controller, namespace)
        }

        mainRouter.get("/clusters/:clusterName/taskmanagers/:taskmanager/metrics").handler { routingContext ->
            handleGetTaskManagerMetrics(routingContext, json, controller, namespace)
        }


        vertx.eventBus().consumer<String>("/cluster/start") { message ->
            handleCommandClusterStart(message, json, controller, cache)
        }

        vertx.eventBus().consumer<String>("/cluster/stop") { message ->
            handlerCommandClusterStop(message, json, controller, cache)
        }

        vertx.eventBus().consumer<String>("/cluster/scale") { message ->
            handleCommandClusterScale(message, json, controller)
        }

        vertx.eventBus().consumer<String>("/cluster/delete") { message ->
            handleCommandClusterDelete(message, json, controller)
        }

        vertx.eventBus().consumer<String>("/cluster/create") { message ->
            handleCommandClusterCreate(message, json, controller)
        }

        vertx.eventBus().consumer<String>("/cluster/update") { message ->
            handleCommandClusterUpdate(message, json, controller)
        }

        vertx.eventBus().consumer<String>("/job/start") { message ->
            handleCommandJobStart(message, json, controller, cache)
        }

        vertx.eventBus().consumer<String>("/job/stop") { message ->
            handleCommandJobStop(message, json, controller, cache)
        }

        vertx.eventBus().consumer<String>("/job/scale") { message ->
            handleCommandJobScale(message, json, controller)
        }

        vertx.eventBus().consumer<String>("/job/delete") { message ->
            handleCommandJobDelete(message, json, controller)
        }

        vertx.eventBus().consumer<String>("/job/create") { message ->
            handleCommandJobCreate(message, json, controller)
        }

        vertx.eventBus().consumer<String>("/job/update") { message ->
            handleCommandJobUpdate(message, json, controller)
        }

        vertx.eventBus().consumer<String>("/job/savepoint/trigger") { message ->
            handleCommandSavepointTrigger(message, json, controller, cache)
        }

        vertx.eventBus().consumer<String>("/job/savepoint/forget") { message ->
            handleCommandSavepointForget(message, json, controller, cache)
        }


        vertx.exceptionHandler {
            error -> logger.error("An error occurred while processing the request", error)
        }

        context.runOnContext {
            cacheAdapter.watchFlinkDeployments(namespace)
            cacheAdapter.watchFlinkClusters(namespace)
            cacheAdapter.watchFlinkJobs(namespace)
            cacheAdapter.watchDeployments(namespace)
            cacheAdapter.watchPods(namespace)

            thread {
                runner.run()
            }
        }

        return vertx.createHttpServer(serverOptions)
            .requestHandler(mainRouter)
            .rxListen(port)
    }

    private fun handleCommandSavepointForget(message: Message<String>, json: JSON, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<JobCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), JobCommand::class.java
                )
            },
            {
                controller.requestForgetSavepoint(
                    it.selector.namespace,
                    it.selector.clusterName,
                    it.selector.jobName,
                    JobContext(cache.getFlinkJob(it.selector.resourceName) ?: throw RuntimeException("Job not found"))
                )
            }
        )
    }

    private fun handleCommandSavepointTrigger(message: Message<String>, json: JSON, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<JobCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), JobCommand::class.java
                )
            },
            {
                controller.requestTriggerSavepoint(
                    it.selector.namespace,
                    it.selector.clusterName,
                    it.selector.jobName,
                    JobContext(cache.getFlinkJob(it.selector.resourceName) ?: throw RuntimeException("Job not found"))
                )
            }
        )
    }

    private fun handleCommandJobUpdate(message: Message<String>, json: JSON, controller: Controller) {
        processCommandAndReplyToSender<JobCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), JobCommand::class.java
                )
            },
            {
                controller.updateFlinkJob(
                    it.selector.namespace,
                    it.selector.clusterName,
                    it.selector.jobName,
                    makeFlinkJob(controller, it.selector.namespace, it.selector.clusterName, it.selector.jobName, it.json)
                )
            }
        )
    }

    private fun handleCommandJobCreate(message: Message<String>, json: JSON, controller: Controller) {
        processCommandAndReplyToSender<JobCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), JobCommand::class.java
                )
            },
            {
                controller.createFlinkJob(
                    it.selector.namespace,
                    it.selector.clusterName,
                    it.selector.jobName,
                    makeFlinkJob(controller, it.selector.namespace, it.selector.clusterName, it.selector.jobName, it.json)
                )
            }
        )
    }

    private fun handleCommandJobDelete(message: Message<String>, json: JSON, controller: Controller) {
        processCommandAndReplyToSender<JobCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), JobCommand::class.java
                )
            },
            {
                controller.deleteFlinkJob(
                    it.selector.namespace,
                    it.selector.clusterName,
                    it.selector.jobName
                )
            }
        )
    }

    private fun handleCommandJobScale(message: Message<String>, json: JSON, controller: Controller) {
        processCommandAndReplyToSender<JobCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), JobCommand::class.java
                )
            },
            {
                controller.requestScaleJob(
                    it.selector.namespace,
                    it.selector.clusterName,
                    it.selector.jobName,
                    json.deserialize(it.json, ScaleJobOptions::class.java)
                )
            }
        )
    }

    private fun handleCommandJobStop(message: Message<String>, json: JSON, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<JobCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), JobCommand::class.java
                )
            },
            {
                controller.requestStopJob(
                    it.selector.namespace,
                    it.selector.clusterName,
                    it.selector.jobName,
                    json.deserialize(it.json, StopOptions::class.java),
                    JobContext(cache.getFlinkJob(it.selector.resourceName) ?: throw RuntimeException("Job not found"))
                )
            }
        )
    }

    private fun handleCommandJobStart(message: Message<String>, json: JSON, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<JobCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), JobCommand::class.java
                )
            },
            {
                controller.requestStartJob(
                    it.selector.namespace,
                    it.selector.clusterName,
                    it.selector.jobName,
                    json.deserialize(it.json, StartOptions::class.java),
                    JobContext(cache.getFlinkJob(it.selector.resourceName) ?: throw RuntimeException("Job not found"))
                )
            }
        )
    }

    private fun handleCommandClusterUpdate(message: Message<String>, json: JSON, controller: Controller) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), ClusterCommand::class.java
                )
            },
            {
                controller.updateFlinkCluster(
                    it.selector.namespace,
                    it.selector.clusterName,
                    makeFlinkCluster(controller, it.selector.namespace, it.selector.clusterName, it.json)
                )
            }
        )
    }

    private fun handleCommandClusterCreate(message: Message<String>, json: JSON, controller: Controller) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), ClusterCommand::class.java
                )
            },
            {
                controller.createFlinkCluster(
                    it.selector.namespace,
                    it.selector.clusterName,
                    makeFlinkCluster(controller, it.selector.namespace, it.selector.clusterName, it.json)
                )
            }
        )
    }

    private fun handleCommandClusterDelete(message: Message<String>, json: JSON, controller: Controller) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), ClusterCommand::class.java
                )
            },
            {
                controller.deleteFlinkCluster(
                    it.selector.namespace,
                    it.selector.clusterName
                )
            }
        )
    }

    private fun handleCommandClusterScale(message: Message<String>, json: JSON, controller: Controller) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), ClusterCommand::class.java
                )
            },
            {
                controller.requestScaleCluster(
                    it.selector.namespace,
                    it.selector.clusterName,
                    json.deserialize(it.json, ScaleClusterOptions::class.java)
                )
            }
        )
    }

    private fun handlerCommandClusterStop(message: Message<String>, json: JSON, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), ClusterCommand::class.java
                )
            },
            {
                controller.requestStopCluster(
                    it.selector.namespace,
                    it.selector.clusterName,
                    json.deserialize(it.json, StopOptions::class.java),
                    ClusterContext(cache.getFlinkCluster(it.selector.resourceName) ?: throw RuntimeException("Cluster not found"))
                )
            }
        )
    }

    private fun handleCommandClusterStart(message: Message<String>, json: JSON, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
            json,
            message,
            {
                json.deserialize(
                    it.body(), ClusterCommand::class.java
                )
            },
            {
                controller.requestStartCluster(
                    it.selector.namespace,
                    it.selector.clusterName,
                    json.deserialize(it.json, StartOptions::class.java),
                    ClusterContext(cache.getFlinkCluster(it.selector.resourceName) ?: throw RuntimeException("Cluster not found"))
                )
            }
        )
    }

    private fun handleGetTaskManagerMetrics(routingContext: RoutingContext, json: JSON, controller: Controller, namespace: String) {
        processRequest(routingContext, json, Function { context ->
            controller.getTaskManagerMetrics(
                namespace, context.pathParam("clusterName"),
                TaskManagerId(context.pathParam("taskmanager"))
            )
        })
    }

    private fun handleGetTaskManagerDetails(routingContext: RoutingContext, json: JSON, controller: Controller, namespace: String) {
        processRequest(routingContext, json, Function { context ->
            controller.getTaskManagerDetails(
                namespace, context.pathParam("clusterName"),
                TaskManagerId(context.pathParam("taskmanager"))
            )
        })
    }

    private fun handleListTaskManagers(routingContext: RoutingContext, json: JSON, controller: Controller, namespace: String) {
        processRequest(routingContext, json, Function { context ->
            controller.getTaskManagersList(
                namespace, context.pathParam("clusterName")
            )
        })
    }

    private fun handleGetJobManagerMetrics(routingContext: RoutingContext, json: JSON, controller: Controller, namespace: String) {
        processRequest(routingContext, json, Function { context ->
            controller.getJobManagerMetrics(
                namespace, context.pathParam("clusterName")
            )
        })
    }

    private fun handleGetJobMetrics(routingContext: RoutingContext, json: JSON, controller: Controller, namespace: String, cache: Cache) {
        processRequest(routingContext, json, Function { context ->
            controller.getJobMetrics(
                namespace,
                context.pathParam("clusterName"),
                context.pathParam("jobName"),
                makeJobContext(cache, context.pathParam("clusterName") + "-" + context.pathParam("jobName"))
            )
        })
    }

    private fun handleGetJobDetails(routingContext: RoutingContext, json: JSON, controller: Controller, namespace: String, cache: Cache) {
        processRequest(routingContext, json, Function { context ->
            controller.getJobDetails(
                namespace,
                context.pathParam("clusterName"),
                context.pathParam("jobName"),
                makeJobContext(cache, context.pathParam("clusterName") + "-" + context.pathParam("jobName"))
            )
        })
    }

    private fun handleGetJobStatus(routingContext: RoutingContext, json: JSON, controller: Controller, namespace: String, cache: Cache) {
        processRequest(routingContext, json, Function { context ->
            controller.getFlinkJobStatus(
                namespace,
                context.pathParam("clusterName"),
                context.pathParam("jobName"),
                makeJobContext(cache, context.pathParam("clusterName") + "-" + context.pathParam("jobName"))
            )
        })
    }

    private fun handleListJobs(routingContext: RoutingContext, json: JSON, cache: Cache) {
        processRequest(routingContext, json, Function { context ->
            Result(ResultStatus.OK, cache.listJobNames().filter {
                it.startsWith(context.pathParam("clusterName") + "-")
            })
        })
    }

    private fun handleGetClusterStatus(routingContext: RoutingContext, json: JSON, controller: Controller, namespace: String, cache: Cache) {
        processRequest(routingContext, json, Function { context ->
            controller.getFlinkClusterStatus(
                namespace,
                context.pathParam("clusterName"),
                makeClusterContext(cache, context.pathParam("clusterName"))
            )
        })
    }

    private fun handleListClusters(routingContext: RoutingContext, json: JSON, cache: Cache) {
        processRequest(routingContext, json, Function {
            Result(ResultStatus.OK, cache.listClusterNames())
        })
    }

    private fun handleClusterCommand(routingContext: RoutingContext, namespace: String, json: JSON, path: String, payloadSupplier: Function<RoutingContext, String>) {
        sendMessageAndWaitForReply(routingContext, namespace, path, BiFunction { context, namespace ->
            json.serialize(ClusterCommand(
                ClusterSelector(namespace = namespace, clusterName = context.pathParam("clusterName")), payloadSupplier.apply(context)
            ))
        })
    }

    private fun handleJobCommand(routingContext: RoutingContext, namespace: String, json: JSON, path: String, payloadSupplier: Function<RoutingContext, String>) {
        sendMessageAndWaitForReply(routingContext, namespace, path, BiFunction { context, namespace ->
            json.serialize(JobCommand(
                JobSelector(namespace = namespace, clusterName = context.pathParam("clusterName"), jobName = context.pathParam("jobName")), payloadSupplier.apply(context)
            ))
        })
    }

    private fun makeClusterContext(cache: Cache, resourceName: String) =
        ClusterContext(cache.getFlinkCluster(resourceName) ?: throw RuntimeException("Cluster not found"))

    private fun makeJobContext(cache: Cache, resourceName: String) =
        JobContext(cache.getFlinkJob(resourceName) ?: throw RuntimeException("Job not found"))

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

    private fun makeFlinkCluster(controller: Controller, namespace: String, clusterName: String, json: String): V1FlinkCluster {
        return controller.makeFlinkCluster(namespace, clusterName, Resource.parseV1FlinkClusterSpec(json))
    }

    private fun makeFlinkJob(controller: Controller, namespace: String, clusterName: String, jobName: String, json: String): V1FlinkJob {
        return controller.makeFlinkJob(namespace, clusterName, jobName, Resource.parseV1FlinkJobSpec(json))
    }

    private fun <R> processRequest(context: RoutingContext, json: JSON, handler: Function<RoutingContext, Result<R>>) {
        Single.just(context)
            .map {
                val result = handler.apply(context)

                if (result.isSuccessful()) {
                    json.serialize(Result(ResultStatus.OK, json.serialize(result.output)))
                } else {
                    json.serialize(Result(ResultStatus.ERROR, null))
                }
            }
            .doOnSuccess {
                context.response().setStatusCode(200).putHeader("content-type", "application/json").end(it)
            }
            .doOnError {
                context.response().setStatusCode(500).putHeader("content-type", "application/json").end(makeError(it))
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
                context.response().setStatusCode(500).putHeader("content-type", "application/json").end(makeError(it))
            }
            .doOnError {
                logger.warn("Can't process request", it)
            }
            .subscribe()
    }

    private fun <T, R> processCommandAndReplyToSender(
        json: JSON,
        message: Message<String>,
        converter: Function<Message<String>, T>,
        handler: Function<T, Result<R>>
    ) {
        Single.just(message)
            .map {
                converter.apply(it)
            }
            .map {
                val result = handler.apply(it)

                if (result.isSuccessful()) {
                    json.serialize(Result(ResultStatus.OK, json.serialize(result.output)))
                } else {
                    json.serialize(Result(ResultStatus.ERROR, null))
                }
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
