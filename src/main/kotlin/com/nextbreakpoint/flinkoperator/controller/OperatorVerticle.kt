package com.nextbreakpoint.flinkoperator.controller

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.operation.JobDetails
import com.nextbreakpoint.flinkoperator.controller.operation.JobManagerMetrics
import com.nextbreakpoint.flinkoperator.controller.operation.JobMetrics
import com.nextbreakpoint.flinkoperator.controller.operation.TaskManagerDetails
import com.nextbreakpoint.flinkoperator.controller.operation.TaskManagerMetrics
import com.nextbreakpoint.flinkoperator.controller.operation.TaskManagersList
import com.nextbreakpoint.flinkoperator.controller.task.CancelJob
import com.nextbreakpoint.flinkoperator.controller.task.ClusterHalted
import com.nextbreakpoint.flinkoperator.controller.task.ClusterRunning
import com.nextbreakpoint.flinkoperator.controller.task.CreateBootstrapJob
import com.nextbreakpoint.flinkoperator.controller.task.CreateResources
import com.nextbreakpoint.flinkoperator.controller.task.CreatingSavepoint
import com.nextbreakpoint.flinkoperator.controller.task.DeleteBootstrapJob
import com.nextbreakpoint.flinkoperator.controller.task.DeleteResources
import com.nextbreakpoint.flinkoperator.controller.task.EraseSavepoint
import com.nextbreakpoint.flinkoperator.controller.task.InitialiseCluster
import com.nextbreakpoint.flinkoperator.controller.task.RescaleCluster
import com.nextbreakpoint.flinkoperator.controller.task.RestartPods
import com.nextbreakpoint.flinkoperator.controller.task.StartJob
import com.nextbreakpoint.flinkoperator.controller.task.StartingCluster
import com.nextbreakpoint.flinkoperator.controller.task.StopJob
import com.nextbreakpoint.flinkoperator.controller.task.StoppingCluster
import com.nextbreakpoint.flinkoperator.controller.task.SuspendCluster
import com.nextbreakpoint.flinkoperator.controller.task.TerminateCluster
import com.nextbreakpoint.flinkoperator.controller.task.TerminatePods
import com.nextbreakpoint.flinkoperator.controller.task.TriggerSavepoint
import com.nextbreakpoint.flinkoperator.controller.task.UpdatingCluster
import io.kubernetes.client.JSON
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1ObjectMeta
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
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
import org.joda.time.DateTime
import rx.Completable
import rx.Observable
import rx.Single
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiFunction
import java.util.function.Consumer
import java.util.function.Function

class OperatorVerticle : AbstractVerticle() {
    companion object {
        private val logger: Logger = Logger.getLogger(OperatorVerticle::class.simpleName)

        private val gson = JSON().gson//Builder().registerTypeAdapter(DateTime::class.java, DateTimeSerializer()).create()

        private val tasksHandlers = mapOf(
            ClusterTask.InitialiseCluster to InitialiseCluster(),
            ClusterTask.TerminatedCluster to TerminateCluster(),
            ClusterTask.SuspendCluster to SuspendCluster(),
            ClusterTask.ClusterHalted to ClusterHalted(),
            ClusterTask.ClusterRunning to ClusterRunning(),
            ClusterTask.StartingCluster to StartingCluster(),
            ClusterTask.StoppingCluster to StoppingCluster(),
            ClusterTask.UpdatingCluster to UpdatingCluster(),
            ClusterTask.RescaleCluster to RescaleCluster(),
            ClusterTask.CreatingSavepoint to CreatingSavepoint(),
            ClusterTask.TriggerSavepoint to TriggerSavepoint(),
            ClusterTask.EraseSavepoint to EraseSavepoint(),
            ClusterTask.CreateResources to CreateResources(),
            ClusterTask.DeleteResources to DeleteResources(),
            ClusterTask.TerminatePods to TerminatePods(),
            ClusterTask.RestartPods to RestartPods(),
            ClusterTask.DeleteBootstrapJob to DeleteBootstrapJob(),
            ClusterTask.CreateBootstrapJob to CreateBootstrapJob(),
            ClusterTask.CancelJob to CancelJob(),
            ClusterTask.StartJob to StartJob(),
            ClusterTask.StopJob to StopJob()
        )
    }

    override fun rxStart(): Completable {
        return createServer(vertx.orCreateContext.config()).toCompletable()
    }

    private fun createServer(config: JsonObject): Single<HttpServer> {
        val port: Int = config.getInteger("port") ?: 4444

        val flinkHostname: String? = config.getString("flink_hostname") ?: null

        val portForward: Int? = config.getInteger("port_forward") ?: null

        val useNodePort: Boolean = config.getBoolean("use_node_port", false)

        val namespace: String = config.getString("namespace") ?: throw RuntimeException("Missing required property namespace")

        val jksKeyStorePath = config.getString("server_keystore_path")

        val jksKeyStoreSecret = config.getString("server_keystore_secret")

        val jksTrustStorePath = config.getString("server_truststore_path")

        val jksTrustStoreSecret = config.getString("server_truststore_secret")

        val serverOptions = createServerOptions(jksKeyStorePath, jksTrustStorePath, jksKeyStoreSecret, jksTrustStoreSecret)

        val flinkOptions = FlinkOptions(
            hostname = flinkHostname,
            portForward = portForward,
            useNodePort = useNodePort
        )

        val kubeClient = KubeClient

        val flinkClient = FlinkClient

        val watch = WatchAdapter(gson, kubeClient)

        val cache = Cache()

        val controller = OperationController(
            flinkOptions,
            flinkClient,
            kubeClient,
            cache,
            tasksHandlers
        )

        val mainRouter = Router.router(vertx)

        val registry = BackendRegistries.getNow("flink-operator")

        val gauges = registerMetrics(registry, namespace)

        val worker = vertx.createSharedWorkerExecutor("execution-queue", 1, 60, TimeUnit.SECONDS)

        mainRouter.route().handler(LoggerHandler.create(true, LoggerFormat.DEFAULT))
        mainRouter.route().handler(BodyHandler.create())
        mainRouter.route().handler(TimeoutHandler.create(120000))

        mainRouter.options("/").handler { context -> context.response().setStatusCode(204).end() }


        mainRouter.put("/cluster/:name/start").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/start", BiFunction { ctx, ns -> gson.toJson(
                com.nextbreakpoint.flinkoperator.controller.core.Message(
                    cache.getClusterId(
                        ns,
                        ctx.pathParam("name")
                    ), ctx.bodyAsString
                )
            ) })
        }

        mainRouter.put("/cluster/:name/stop").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/stop", BiFunction { ctx, ns -> gson.toJson(
                com.nextbreakpoint.flinkoperator.controller.core.Message(
                    cache.getClusterId(
                        ns,
                        ctx.pathParam("name")
                    ), ctx.bodyAsString
                )
            ) })
        }

        mainRouter.put("/cluster/:name/scale").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/scale", BiFunction { ctx, ns -> gson.toJson(
                com.nextbreakpoint.flinkoperator.controller.core.Message(
                    cache.getClusterId(
                        ns,
                        ctx.pathParam("name")
                    ), ctx.bodyAsString
                )
            ) })
        }

        mainRouter.put("/cluster/:name/savepoint").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/savepoint/trigger", BiFunction { ctx, ns -> gson.toJson(
                com.nextbreakpoint.flinkoperator.controller.core.Message(
                    cache.getClusterId(
                        ns,
                        ctx.pathParam("name")
                    ), ctx.bodyAsString
                )
            ) })
        }

        mainRouter.delete("/cluster/:name/savepoint").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/savepoint/forget", BiFunction { ctx, ns -> gson.toJson(
                com.nextbreakpoint.flinkoperator.controller.core.Message(
                    cache.getClusterId(
                        ns,
                        ctx.pathParam("name")
                    ), "{}"
                )
            ) })
        }


        mainRouter.get("/cluster/:name/status").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(controller.getClusterStatus(cache.getClusterId(namespace, context.pathParam("name")))) })
        }

        mainRouter.get("/cluster/:name/job/details").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(
                JobDetails(flinkOptions, flinkClient, kubeClient).execute(cache.getClusterId(namespace, context.pathParam("name")), null)
            ) })
        }

        mainRouter.get("/cluster/:name/job/metrics").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(
                JobMetrics(flinkOptions, flinkClient, kubeClient).execute(cache.getClusterId(namespace, context.pathParam("name")), null)
            ) })
        }

        mainRouter.get("/cluster/:name/jobmanager/metrics").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(
                JobManagerMetrics(flinkOptions, flinkClient, kubeClient).execute(cache.getClusterId(namespace, context.pathParam("name")), null)
            ) })
        }

        mainRouter.get("/cluster/:name/taskmanagers").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(
                TaskManagersList(flinkOptions, flinkClient, kubeClient).execute(cache.getClusterId(namespace, context.pathParam("name")), null)
            ) })
        }

        mainRouter.get("/cluster/:name/taskmanagers/:taskmanager/details").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(
                TaskManagerDetails(flinkOptions, flinkClient, kubeClient).execute(cache.getClusterId(namespace, context.pathParam("name")), TaskManagerId(context.pathParam("taskmanager")))
            ) })
        }

        mainRouter.get("/cluster/:name/taskmanagers/:taskmanager/metrics").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(
                TaskManagerMetrics(flinkOptions, flinkClient, kubeClient).execute(cache.getClusterId(namespace, context.pathParam("name")), TaskManagerId(context.pathParam("taskmanager")))
            ) })
        }


        mainRouter.delete("/cluster/:name").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/delete", BiFunction { context, namespace -> gson.toJson(
                cache.getClusterId(namespace, context.pathParam("name"))
            ) })
        }

        mainRouter.post("/cluster/:name").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/create", BiFunction { context, namespace -> gson.toJson(
                makeV1FlinkCluster(context, namespace)
            ) })
        }


        vertx.eventBus().consumer<String>("/cluster/start") { message ->
            handleCommand<com.nextbreakpoint.flinkoperator.controller.core.Message>(message, worker, Function { gson.fromJson(it.body(), com.nextbreakpoint.flinkoperator.controller.core.Message::class.java) }, Function {
                gson.toJson(controller.requestStartCluster(it.clusterId, gson.fromJson(it.json, StartOptions::class.java)))
            })
        }

        vertx.eventBus().consumer<String>("/cluster/stop") { message ->
            handleCommand<com.nextbreakpoint.flinkoperator.controller.core.Message>(message, worker, Function { gson.fromJson(it.body(), com.nextbreakpoint.flinkoperator.controller.core.Message::class.java) }, Function {
                gson.toJson(controller.requestStopCluster(it.clusterId, gson.fromJson(it.json, StopOptions::class.java)))
            })
        }

        vertx.eventBus().consumer<String>("/cluster/scale") { message ->
            handleCommand<com.nextbreakpoint.flinkoperator.controller.core.Message>(message, worker, Function { gson.fromJson(it.body(), com.nextbreakpoint.flinkoperator.controller.core.Message::class.java) }, Function {
                gson.toJson(controller.requestScaleCluster(it.clusterId, gson.fromJson(it.json, ScaleOptions::class.java)))
            })
        }

        vertx.eventBus().consumer<String>("/cluster/delete") { message ->
            handleCommand<ClusterId>(message, worker, Function { gson.fromJson(it.body(), ClusterId::class.java) }, Function {
                gson.toJson(controller.deleteFlinkCluster(it))
            })
        }

        vertx.eventBus().consumer<String>("/cluster/create") { message ->
            handleCommand<V1FlinkCluster>(message, worker, Function { gson.fromJson(it.body(), V1FlinkCluster::class.java) }, Function {
                gson.toJson(controller.createFlinkCluster(ClusterId(it.metadata.namespace, it.metadata.name, ""), it)) })
        }

        vertx.eventBus().consumer<String>("/cluster/savepoint/trigger") { message ->
            handleCommand<com.nextbreakpoint.flinkoperator.controller.core.Message>(message, worker, Function { gson.fromJson(it.body(), com.nextbreakpoint.flinkoperator.controller.core.Message::class.java) }, Function {
                gson.toJson(controller.createSavepoint(it.clusterId))
            })
        }

        vertx.eventBus().consumer<String>("/cluster/savepoint/forget") { message ->
            handleCommand<com.nextbreakpoint.flinkoperator.controller.core.Message>(message, worker, Function { gson.fromJson(it.body(), com.nextbreakpoint.flinkoperator.controller.core.Message::class.java) }, Function {
                gson.toJson(controller.forgetSavepoint(it.clusterId))
            })
        }

        vertx.eventBus().consumer<String>("/resource/flinkcluster/change") { message ->
            handleEvent<V1FlinkCluster>(message, Function { gson.fromJson(it.body(), V1FlinkCluster::class.java) }, Consumer { cache.onFlinkClusterChanged(it) })
        }

        vertx.eventBus().consumer<String>("/resource/flinkcluster/delete") { message ->
            handleEvent<V1FlinkCluster>(message, Function { gson.fromJson(it.body(), V1FlinkCluster::class.java) }, Consumer { cache.onFlinkClusterDeleted(it) })
        }

        vertx.eventBus().consumer<String>("/resource/flinkcluster/deleteAll") { _ ->
            cache.onFlinkClusterDeleteAll()
        }

        vertx.eventBus().consumer<String>("/resource/service/change") { message ->
            handleEvent<V1Service>(message, Function { gson.fromJson(it.body(), V1Service::class.java) }, Consumer { cache.onServiceChanged(it) })
        }

        vertx.eventBus().consumer<String>("/resource/service/delete") { message ->
            handleEvent<V1Service>(message, Function { gson.fromJson(it.body(), V1Service::class.java) }, Consumer { cache.onServiceDeleted(it) })
        }

        vertx.eventBus().consumer<String>("/resource/service/deleteAll") { _ ->
            cache.onServiceDeleteAll()
        }

        vertx.eventBus().consumer<String>("/resource/job/change") { message ->
            handleEvent<V1Job>(message, Function { gson.fromJson(it.body(), V1Job::class.java) }, Consumer { cache.onJobChanged(it) })
        }

        vertx.eventBus().consumer<String>("/resource/job/delete") { message ->
            handleEvent<V1Job>(message, Function { gson.fromJson(it.body(), V1Job::class.java) }, Consumer { cache.onJobDeleted(it) })
        }

        vertx.eventBus().consumer<String>("/resource/job/deleteAll") { _ ->
            cache.onJobDeleteAll()
        }

        vertx.eventBus().consumer<String>("/resource/statefulset/change") { message ->
            handleEvent<V1StatefulSet>(message, Function { gson.fromJson(it.body(), V1StatefulSet::class.java) }, Consumer { cache.onStatefulSetChanged(it) })
        }

        vertx.eventBus().consumer<String>("/resource/statefulset/delete") { message ->
            handleEvent<V1StatefulSet>(message, Function { gson.fromJson(it.body(), V1StatefulSet::class.java) }, Consumer { cache.onStatefulSetDeleted(it) })
        }

        vertx.eventBus().consumer<String>("/resource/statefulset/deleteAll") { _ ->
            cache.onStatefulSetDeleteAll()
        }

        vertx.eventBus().consumer<String>("/resource/persistentvolumeclaim/change") { message ->
            handleEvent<V1PersistentVolumeClaim>(message, Function { gson.fromJson(it.body(), V1PersistentVolumeClaim::class.java) }, Consumer { cache.onPersistentVolumeClaimChanged(it) })
        }

        vertx.eventBus().consumer<String>("/resource/persistentvolumeclaim/delete") { message ->
            handleEvent<V1PersistentVolumeClaim>(message, Function { gson.fromJson(it.body(), V1PersistentVolumeClaim::class.java) }, Consumer { cache.onPersistentVolumeClaimDeleted(it) })
        }

        vertx.eventBus().consumer<String>("/resource/persistentvolumeclaim/deleteAll") { _ ->
            cache.onPersistentVolumeClaimDeleteAll()
        }


        vertx.eventBus().consumer<String>("/resource/cluster/update") { message ->
            handleCommandNoReply<com.nextbreakpoint.flinkoperator.controller.core.Message>(message, worker, Function { gson.fromJson(it.body(), com.nextbreakpoint.flinkoperator.controller.core.Message::class.java) }, Function {
                controller.updateClusterStatus(it.clusterId)

                null
            })
        }

        vertx.eventBus().consumer<String>("/resource/cluster/forget") { message ->
            handleCommandNoReply<com.nextbreakpoint.flinkoperator.controller.core.Message>(message, worker, Function { gson.fromJson(it.body(), com.nextbreakpoint.flinkoperator.controller.core.Message::class.java) }, Function {
                controller.terminatePods(it.clusterId)

                val result = controller.arePodsTerminated(it.clusterId)

                if (result.status == ResultStatus.SUCCESS) {
                    controller.deleteClusterResources(it.clusterId)
                }

                null
            })
        }


        vertx.exceptionHandler { error -> logger.error("An error occurred while processing the request", error) }

        context.runOnContext {
            watch.watchFlinkClusters(context, namespace)
        }

        context.runOnContext {
            watch.watchServices(context, namespace)
        }

        context.runOnContext {
            watch.watchJobs(context, namespace)
        }

        context.runOnContext {
            watch.watchStatefulSets(context, namespace)
        }

        context.runOnContext {
            watch.watchPersistentVolumeClaims(context, namespace)
        }

        vertx.setPeriodic(5000L) {
            updateMetrics(cache, gauges)

            doUpdateClusters(cache)

            doDeleteOrphans(cache)
        }

        return vertx.createHttpServer(serverOptions).requestHandler(mainRouter).rxListen(port)
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

    private fun updateMetrics(resourcesCache: Cache, gauges: Map<ClusterStatus, AtomicInteger>) {
        val counters = resourcesCache.getFlinkClusters()
            .foldRight(mutableMapOf<ClusterStatus, Int>()) { flinkCluster, counters ->
                val status = Status.getClusterStatus(flinkCluster)
                counters.compute(status) { _, value -> if (value != null) value + 1 else 1 }
                counters
            }

        ClusterStatus.values().forEach {
            gauges.get(it)?.set(counters.get(it) ?: 0)
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

    private fun handleRequest(context: RoutingContext, handler: Function<RoutingContext, String>) {
        Single.just(context)
            .map { handler.apply(context) }
            .doOnSuccess { context.response().setStatusCode(200).putHeader("content-type", "application/json").end(it) }
            .doOnError { context.response().setStatusCode(500).end(makeError(it)) }
            .doOnError { logger.warn("Can't process request", it) }
            .subscribe()
    }

    private fun handleRequest(context: RoutingContext, namespace: String, address: String, handler: BiFunction<RoutingContext, String, String>) {
        Single.just(context)
            .map { handler.apply(context, namespace) }
            .flatMap { context.vertx().eventBus().rxRequest<String>(address, it, DeliveryOptions().setSendTimeout(30000)) }
            .doOnSuccess { context.response().setStatusCode(200).putHeader("content-type", "application/json").end(it.body()) }
            .doOnError { context.response().setStatusCode(500).end(makeError(it)) }
            .doOnError { logger.warn("Can't process request", it) }
            .subscribe()
    }

    private fun <T> handleCommand(message: Message<String>, worker: WorkerExecutor, converter: Function<Message<String>, T>, handler: Function<T, String>) {
        Single.just(message)
            .map { converter.apply(it) }
            .flatMap { worker.rxExecuteBlocking<String> { future -> future.complete(handler.apply(it)) } }
            .doOnError { logger.error("Can't process message [address=${message.address()}]", it) }
            .onErrorReturn { makeError(it) }
            .doOnSuccess { message.reply(it) }
            .doOnError { logger.error("Can't send response [address=${message.address()}]", it) }
            .subscribe()
    }

    private fun <T> handleCommandNoReply(message: Message<String>, worker: WorkerExecutor, converter: Function<Message<String>, T>, handler: Function<T, Void?>) {
        Single.just(message)
            .map { converter.apply(it) }
            .flatMap { worker.rxExecuteBlocking<Void?> { future -> future.complete(handler.apply(it)) } }
            .doOnError { logger.error("Can't process message [address=${message.address()}]", it) }
            .subscribe()
    }

    private fun <T> handleEvent(message: Message<String>, converter: Function<Message<String>, T>, handler: Consumer<T>) {
        Single.just(message)
            .map { converter.apply(it) }
            .doOnSuccess { handler.accept(it) }
            .doOnError { logger.error("Can't process message [address=${message.address()}]", it) }
            .subscribe()
    }

    private fun makeClusterId(flinkCluster: V1FlinkCluster) =
        ClusterId(namespace = flinkCluster.metadata.namespace, name = flinkCluster.metadata.name, uuid = flinkCluster.metadata.uid)

    private fun doUpdateClusters(cache: Cache) {
        cache.getFlinkClusters().forEach {
            vertx.eventBus().publish("/resource/cluster/update", gson.toJson(
                com.nextbreakpoint.flinkoperator.controller.core.Message(makeClusterId(it), "{}")
            ))
        }
    }

    private fun doDeleteOrphans(cache: Cache) {
        cache.getOrphanedClusters().forEach {
            vertx.eventBus().publish("/resource/cluster/forget", gson.toJson(
                com.nextbreakpoint.flinkoperator.controller.core.Message(it, "{}")
            ))
        }
    }
}
