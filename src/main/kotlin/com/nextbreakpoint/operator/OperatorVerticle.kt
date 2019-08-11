package com.nextbreakpoint.operator

import com.google.gson.GsonBuilder
import com.nextbreakpoint.common.FlinkClusterSpecification
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.StartOptions
import com.nextbreakpoint.common.model.StopOptions
import com.nextbreakpoint.common.model.TaskManagerId
import com.nextbreakpoint.model.DateTimeSerializer
import com.nextbreakpoint.model.V1FlinkCluster
import com.nextbreakpoint.operator.command.JobDetails
import com.nextbreakpoint.operator.command.JobManagerMetrics
import com.nextbreakpoint.operator.command.JobMetrics
import com.nextbreakpoint.operator.command.TaskManagerDetails
import com.nextbreakpoint.operator.command.TaskManagerMetrics
import com.nextbreakpoint.operator.command.TaskManagersList
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
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiFunction
import java.util.function.Consumer
import java.util.function.Function

class OperatorVerticle : AbstractVerticle() {
    companion object {
        private val logger: Logger = Logger.getLogger(OperatorVerticle::class.simpleName)

        private val gson = GsonBuilder().registerTypeAdapter(DateTime::class.java, DateTimeSerializer()).create()
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

        val serverOptions = HttpServerOptions()

        if (jksKeyStorePath != null && jksTrustStorePath != null) {
            logger.info("Enabling HTTPS with required client auth")

            serverOptions
                .setSsl(true)
                .setSni(false)
                .setKeyStoreOptions(JksOptions().setPath(jksKeyStorePath).setPassword(jksKeyStoreSecret))
                .setTrustStoreOptions(JksOptions().setPath(jksTrustStorePath).setPassword(jksTrustStoreSecret))
                .setClientAuth(ClientAuth.REQUIRED)
        } else {
            logger.warn("HTTPS not enabled!")
        }

        val watch = OperatorWatch(gson)

        val flinkOptions = FlinkOptions(
            hostname = flinkHostname,
            portForward = portForward,
            useNodePort = useNodePort
        )

        val controller = OperatorController(flinkOptions)

        val resourcesCache = OperatorCache()

        val mainRouter = Router.router(vertx)

        val registry = BackendRegistries.getNow("flink-operator")

        val gauges = registerMetrics(registry, namespace)

        val worker = vertx.createSharedWorkerExecutor("execution-queue", 1)

        mainRouter.route().handler(LoggerHandler.create(true, LoggerFormat.DEFAULT))
        mainRouter.route().handler(BodyHandler.create())
        mainRouter.route().handler(TimeoutHandler.create(120000))

        mainRouter.options("/").handler { context -> context.response().setStatusCode(204).end() }

        mainRouter.put("/cluster/:name/start").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/start", BiFunction { context, namespace -> gson.toJson(OperatorMessage(resourcesCache.getClusterIdentity(namespace, context.pathParam("name")), context.bodyAsString)) })
        }

        mainRouter.put("/cluster/:name/stop").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/stop", BiFunction { context, namespace -> gson.toJson(OperatorMessage(resourcesCache.getClusterIdentity(namespace, context.pathParam("name")), context.bodyAsString)) })
        }

        mainRouter.put("/cluster/:name/savepoint").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/savepoint", BiFunction { context, namespace -> gson.toJson(OperatorMessage(resourcesCache.getClusterIdentity(namespace, context.pathParam("name")), context.bodyAsString)) })
        }


        mainRouter.get("/cluster/:name/status").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(controller.getClusterStatus(resourcesCache.getClusterIdentity(namespace, context.pathParam("name")), resourcesCache)) })
        }

        mainRouter.get("/cluster/:name/job/details").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(JobDetails(flinkOptions).execute(resourcesCache.getClusterIdentity(namespace, context.pathParam("name")), null)) })
        }

        mainRouter.get("/cluster/:name/job/metrics").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(JobMetrics(flinkOptions).execute(resourcesCache.getClusterIdentity(namespace, context.pathParam("name")), null)) })
        }

        mainRouter.get("/cluster/:name/jobmanager/metrics").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(JobManagerMetrics(flinkOptions).execute(resourcesCache.getClusterIdentity(namespace, context.pathParam("name")), null)) })
        }

        mainRouter.get("/cluster/:name/taskmanagers").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(TaskManagersList(flinkOptions).execute(resourcesCache.getClusterIdentity(namespace, context.pathParam("name")), null)) })
        }

        mainRouter.get("/cluster/:name/taskmanagers/:taskmanager/details").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(TaskManagerDetails(flinkOptions).execute(resourcesCache.getClusterIdentity(namespace, context.pathParam("name")), TaskManagerId(context.pathParam("taskmanager")))) })
        }

        mainRouter.get("/cluster/:name/taskmanagers/:taskmanager/metrics").handler { routingContext ->
            handleRequest(routingContext, Function { context -> gson.toJson(TaskManagerMetrics(flinkOptions).execute(resourcesCache.getClusterIdentity(namespace, context.pathParam("name")), TaskManagerId(context.pathParam("taskmanager")))) })
        }


        mainRouter.delete("/cluster/:name").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/delete", BiFunction { context, namespace -> gson.toJson(resourcesCache.getClusterIdentity(namespace, context.pathParam("name"))) })
        }

        mainRouter.post("/cluster/:name").handler { routingContext ->
            handleRequest(routingContext, namespace, "/cluster/create", BiFunction { context, namespace -> gson.toJson(makeV1FlinkCluster(context, namespace)) })
        }


        vertx.eventBus().consumer<String>("/cluster/start") { message ->
            handleCommand<OperatorMessage>(message, worker, Function { gson.fromJson(it.body(), OperatorMessage::class.java) }, Function {
                gson.toJson(controller.startCluster(it.clusterId, gson.fromJson(it.json, StartOptions::class.java), resourcesCache))
            })
        }

        vertx.eventBus().consumer<String>("/cluster/stop") { message ->
            handleCommand<OperatorMessage>(message, worker, Function { gson.fromJson(it.body(), OperatorMessage::class.java) }, Function {
                gson.toJson(controller.stopCluster(it.clusterId, gson.fromJson(it.json, StopOptions::class.java), resourcesCache))
            })
        }

        vertx.eventBus().consumer<String>("/cluster/delete") { message ->
            handleCommand<ClusterId>(message, worker, Function { gson.fromJson(it.body(), ClusterId::class.java) }, Function {
                gson.toJson(controller.deleteFlinkCluster(it))
            })
        }

        vertx.eventBus().consumer<String>("/cluster/create") { message ->
            handleCommand<V1FlinkCluster>(message, worker, Function { gson.fromJson(it.body(), V1FlinkCluster::class.java) }, Function {
                gson.toJson(controller.createFlinkCluster(ClusterId(it.metadata.namespace, it.metadata.name, ""), it))
            })
        }

        vertx.eventBus().consumer<String>("/cluster/savepoint") { message ->
            handleCommand<OperatorMessage>(message, worker, Function { gson.fromJson(it.body(), OperatorMessage::class.java) }, Function {
                gson.toJson(controller.createSavepoint(it.clusterId, resourcesCache))
            })
        }


        vertx.eventBus().consumer<String>("/resource/flinkcluster/change") { message ->
            handleEvent<V1FlinkCluster>(message, Function { gson.fromJson(it.body(), V1FlinkCluster::class.java) }, Consumer { resourcesCache.onFlinkClusterChanged(it) })
        }

        vertx.eventBus().consumer<String>("/resource/flinkcluster/delete") { message ->
            handleEvent<V1FlinkCluster>(message, Function { gson.fromJson(it.body(), V1FlinkCluster::class.java) }, Consumer { resourcesCache.onFlinkClusterDeleted(it) })
        }

        vertx.eventBus().consumer<String>("/resource/flinkcluster/deleteAll") { _ ->
            resourcesCache.onFlinkClusterDeleteAll()
        }

        vertx.eventBus().consumer<String>("/resource/service/change") { message ->
            handleEvent<V1Service>(message, Function { gson.fromJson(it.body(), V1Service::class.java) }, Consumer { resourcesCache.onServiceChanged(it) })
        }

        vertx.eventBus().consumer<String>("/resource/service/delete") { message ->
            handleEvent<V1Service>(message, Function { gson.fromJson(it.body(), V1Service::class.java) }, Consumer { resourcesCache.onServiceDeleted(it) })
        }

        vertx.eventBus().consumer<String>("/resource/service/deleteAll") { _ ->
            resourcesCache.onServiceDeleteAll()
        }

        vertx.eventBus().consumer<String>("/resource/job/change") { message ->
            handleEvent<V1Job>(message, Function { gson.fromJson(it.body(), V1Job::class.java) }, Consumer { resourcesCache.onJobChanged(it) })
        }

        vertx.eventBus().consumer<String>("/resource/job/delete") { message ->
            handleEvent<V1Job>(message, Function { gson.fromJson(it.body(), V1Job::class.java) }, Consumer { resourcesCache.onJobDeleted(it) })
        }

        vertx.eventBus().consumer<String>("/resource/job/deleteAll") { _ ->
            resourcesCache.onJobDeleteAll()
        }

        vertx.eventBus().consumer<String>("/resource/statefulset/change") { message ->
            handleEvent<V1StatefulSet>(message, Function { gson.fromJson(it.body(), V1StatefulSet::class.java) }, Consumer { resourcesCache.onStatefulSetChanged(it) })
        }

        vertx.eventBus().consumer<String>("/resource/statefulset/delete") { message ->
            handleEvent<V1StatefulSet>(message, Function { gson.fromJson(it.body(), V1StatefulSet::class.java) }, Consumer { resourcesCache.onStatefulSetDeleted(it) })
        }

        vertx.eventBus().consumer<String>("/resource/statefulset/deleteAll") { _ ->
            resourcesCache.onStatefulSetDeleteAll()
        }

        vertx.eventBus().consumer<String>("/resource/persistentvolumeclaim/change") { message ->
            handleEvent<V1PersistentVolumeClaim>(message, Function { gson.fromJson(it.body(), V1PersistentVolumeClaim::class.java) }, Consumer { resourcesCache.onPersistentVolumeClaimChanged(it) })
        }

        vertx.eventBus().consumer<String>("/resource/persistentvolumeclaim/delete") { message ->
            handleEvent<V1PersistentVolumeClaim>(message, Function { gson.fromJson(it.body(), V1PersistentVolumeClaim::class.java) }, Consumer { resourcesCache.onPersistentVolumeClaimDeleted(it) })
        }

        vertx.eventBus().consumer<String>("/resource/persistentvolumeclaim/deleteAll") { _ ->
            resourcesCache.onPersistentVolumeClaimDeleteAll()
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
            updateMetrics(resourcesCache, gauges)

            doUpdateClusters(controller, resourcesCache, worker)

            doDeleteOrphans(controller, resourcesCache, worker)
        }

        return vertx.createHttpServer(serverOptions).requestHandler(mainRouter).rxListen(port)
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

    private fun updateMetrics(resourcesCache: OperatorCache, gauges: Map<ClusterStatus, AtomicInteger>) {
        val counters = resourcesCache.getFlinkClusters()
            .foldRight(mutableMapOf<ClusterStatus, Int>()) { flinkCluster, counters ->
                val status = OperatorAnnotations.getClusterStatus(flinkCluster)
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
        val flinkClusterSpec = FlinkClusterSpecification.parse(context.bodyAsString)
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

    private fun <T> handleEvent(message: Message<String>, converter: Function<Message<String>, T>, handler: Consumer<T>) {
        Single.just(message)
            .map { converter.apply(it) }
            .doOnSuccess { handler.accept(it) }
            .doOnError { logger.error("Can't process message [address=${message.address()}]", it) }
            .subscribe()
    }

    private fun makeClusterId(flinkCluster: V1FlinkCluster) =
        ClusterId(namespace = flinkCluster.metadata.namespace, name = flinkCluster.metadata.name, uuid = "")

    private fun doUpdateClusters(
        controller: OperatorController,
        status: OperatorCache,
        worker: WorkerExecutor
    ) {
        val observable = Observable.from(status.updateClusters()).map { pair ->
            Runnable {
                controller.updateClusterStatus(makeClusterId(pair.first), pair.first, pair.second)
            }
        }

        worker.rxExecuteBlocking<Void> { future ->
            observable.doOnNext { it.run() }.subscribe({ future.complete() }, { e -> future.fail(e) })
        }.doOnError {
            logger.error("Can't update cluster resources", it)
        }.subscribe()
    }

    private fun doDeleteOrphans(
        controller: OperatorController,
        status: OperatorCache,
        worker: WorkerExecutor
    ) {
        val observable = Observable.from(status.deleteOrphans()).map { clusterId ->
            Runnable {
                controller.terminatePods(clusterId)
                val result = controller.arePodsTerminated(clusterId)
                if (result.status == ResultStatus.SUCCESS) {
                    controller.deleteClusterResources(clusterId)
                }
            }
        }

        worker.rxExecuteBlocking<Void> { future ->
            observable.doOnNext { it.run() }.subscribe({ _ -> future.complete() }, { e -> future.fail(e) })
        }.doOnError {
            logger.error("Can't delete orphaned resources", it)
        }.subscribe()
    }
}
