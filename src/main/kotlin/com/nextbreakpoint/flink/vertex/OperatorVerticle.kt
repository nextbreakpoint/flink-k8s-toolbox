package com.nextbreakpoint.flink.vertex

import com.nextbreakpoint.flink.common.ClusterSelector
import com.nextbreakpoint.flink.common.DeploymentSelector
import com.nextbreakpoint.flink.common.JobSelector
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.common.ScaleJobOptions
import com.nextbreakpoint.flink.common.ServerConfig
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.common.TaskManagerId
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.controller.core.ClusterContext
import com.nextbreakpoint.flink.k8s.controller.core.DeploymentContext
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.operator.core.Cache
import com.nextbreakpoint.flink.vertex.core.ClusterCommand
import com.nextbreakpoint.flink.vertex.core.DeploymentCommand
import com.nextbreakpoint.flink.vertex.core.JobCommand
import io.kubernetes.client.openapi.JSON
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.http.ClientAuth
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.net.JksOptions
import io.vertx.ext.web.handler.LoggerFormat
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.eventbus.Message
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.RoutingContext
import io.vertx.rxjava.ext.web.handler.BodyHandler
import io.vertx.rxjava.ext.web.handler.LoggerHandler
import io.vertx.rxjava.ext.web.handler.TimeoutHandler
import rx.Completable
import rx.Single
import java.util.function.BiFunction
import java.util.function.Function
import java.util.logging.Level
import java.util.logging.Logger

class OperatorVerticle(
    private val namespace: String,
    private val cache: Cache,
    private val controller: Controller,
    private val serverConfig: ServerConfig
) : AbstractVerticle() {
    companion object {
        private val logger: Logger = Logger.getLogger(OperatorVerticle::class.simpleName)

        private val json = JSON()
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


        mainRouter.delete("/deployments/:deploymentName").handler { routingContext ->
            handleDeploymentCommand(routingContext, namespace, "/deployment/delete") { context -> "{}" }
        }

        mainRouter.post("/deployments/:deploymentName").handler { routingContext ->
            handleDeploymentCommand(routingContext, namespace, "/deployment/create") { context -> context.bodyAsString }
        }

        mainRouter.put("/deployments/:deploymentName").handler { routingContext ->
            handleDeploymentCommand(routingContext, namespace, "/deployment/update") { context -> context.bodyAsString }
        }


        mainRouter.put("/clusters/:clusterName/start").handler { routingContext ->
            handleClusterCommand(routingContext, namespace, "/cluster/start") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/stop").handler { routingContext ->
            handleClusterCommand(routingContext, namespace, "/cluster/stop") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/scale").handler { routingContext ->
            handleClusterCommand(routingContext, namespace, "/cluster/scale") { context -> context.bodyAsString }
        }

        mainRouter.delete("/clusters/:clusterName").handler { routingContext ->
            handleClusterCommand(routingContext, namespace, "/cluster/delete") { context -> "{}" }
        }

        mainRouter.post("/clusters/:clusterName").handler { routingContext ->
            handleClusterCommand(routingContext, namespace, "/cluster/create") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName").handler { routingContext ->
            handleClusterCommand(routingContext, namespace, "/cluster/update") { context -> context.bodyAsString }
        }


        mainRouter.put("/clusters/:clusterName/jobs/:jobName/start").handler { routingContext ->
            handleJobCommand(routingContext, namespace, "/job/start") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/jobs/:jobName/stop").handler { routingContext ->
            handleJobCommand(routingContext, namespace, "/job/stop") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/jobs/:jobName/scale").handler { routingContext ->
            handleJobCommand(routingContext, namespace, "/job/scale") { context -> context.bodyAsString }
        }

        mainRouter.delete("/clusters/:clusterName/jobs/:jobName").handler { routingContext ->
            handleJobCommand(routingContext, namespace, "/job/delete") { context -> "{}" }
        }

        mainRouter.post("/clusters/:clusterName/jobs/:jobName").handler { routingContext ->
            handleJobCommand(routingContext, namespace, "/job/create") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/jobs/:jobName").handler { routingContext ->
            handleJobCommand(routingContext, namespace, "/job/update") { context -> context.bodyAsString }
        }

        mainRouter.put("/clusters/:clusterName/jobs/:jobName/savepoint").handler { routingContext ->
            handleJobCommand(routingContext, namespace, "/job/savepoint/trigger") { context -> context.bodyAsString }
        }

        mainRouter.delete("/clusters/:clusterName/jobs/:jobName/savepoint").handler { routingContext ->
            handleJobCommand(routingContext, namespace, "/job/savepoint/forget") { context -> "{}" }
        }


        mainRouter.get("/deployments").handler { routingContext ->
            handleListDeployments(routingContext, cache)
        }

        mainRouter.get("/deployments/:clusterName/status").handler { routingContext ->
            handleGetDeploymentStatus(routingContext, controller, namespace, cache)
        }

        mainRouter.get("/clusters").handler { routingContext ->
            handleListClusters(routingContext, cache)
        }

        mainRouter.get("/clusters/:clusterName/status").handler { routingContext ->
            handleGetClusterStatus(routingContext, controller, namespace, cache)
        }

        mainRouter.get("/clusters/:clusterName/jobs").handler { routingContext ->
            handleListJobs(routingContext, cache)
        }

        mainRouter.get("/clusters/:clusterName/jobs/:jobName/status").handler { routingContext ->
            handleGetJobStatus(routingContext, controller, namespace, cache)
        }

        mainRouter.get("/clusters/:clusterName/jobs/:jobName/details").handler { routingContext ->
            handleGetJobDetails(routingContext, controller, namespace, cache)
        }

        mainRouter.get("/clusters/:clusterName/jobs/:jobName/metrics").handler { routingContext ->
            handleGetJobMetrics(routingContext, controller, namespace, cache)
        }

        mainRouter.get("/clusters/:clusterName/jobmanager/metrics").handler { routingContext ->
            handleGetJobManagerMetrics(routingContext, controller, namespace)
        }

        mainRouter.get("/clusters/:clusterName/taskmanagers").handler { routingContext ->
            handleListTaskManagers(routingContext, controller, namespace)
        }

        mainRouter.get("/clusters/:clusterName/taskmanagers/:taskmanager/details").handler { routingContext ->
            handleGetTaskManagerDetails(routingContext, controller, namespace)
        }

        mainRouter.get("/clusters/:clusterName/taskmanagers/:taskmanager/metrics").handler { routingContext ->
            handleGetTaskManagerMetrics(routingContext, controller, namespace)
        }


        vertx.eventBus().consumer<String>("/deployment/delete") { message ->
            handleCommandDeploymentDelete(message, controller)
        }

        vertx.eventBus().consumer<String>("/deployment/create") { message ->
            handleCommandDeploymentCreate(message, controller)
        }

        vertx.eventBus().consumer<String>("/deployment/update") { message ->
            handleCommandDeploymentUpdate(message, controller)
        }

        vertx.eventBus().consumer<String>("/cluster/start") { message ->
            handleCommandClusterStart(message, controller, cache)
        }

        vertx.eventBus().consumer<String>("/cluster/stop") { message ->
            handlerCommandClusterStop(message, controller, cache)
        }

        vertx.eventBus().consumer<String>("/cluster/scale") { message ->
            handleCommandClusterScale(message, controller)
        }

        vertx.eventBus().consumer<String>("/cluster/delete") { message ->
            handleCommandClusterDelete(message, controller)
        }

        vertx.eventBus().consumer<String>("/cluster/create") { message ->
            handleCommandClusterCreate(message, controller)
        }

        vertx.eventBus().consumer<String>("/cluster/update") { message ->
            handleCommandClusterUpdate(message, controller)
        }

        vertx.eventBus().consumer<String>("/job/start") { message ->
            handleCommandJobStart(message, controller, cache)
        }

        vertx.eventBus().consumer<String>("/job/stop") { message ->
            handleCommandJobStop(message, controller, cache)
        }

        vertx.eventBus().consumer<String>("/job/scale") { message ->
            handleCommandJobScale(message, controller)
        }

        vertx.eventBus().consumer<String>("/job/delete") { message ->
            handleCommandJobDelete(message, controller)
        }

        vertx.eventBus().consumer<String>("/job/create") { message ->
            handleCommandJobCreate(message, controller)
        }

        vertx.eventBus().consumer<String>("/job/update") { message ->
            handleCommandJobUpdate(message, controller)
        }

        vertx.eventBus().consumer<String>("/job/savepoint/trigger") { message ->
            handleCommandSavepointTrigger(message, controller, cache)
        }

        vertx.eventBus().consumer<String>("/job/savepoint/forget") { message ->
            handleCommandSavepointForget(message, controller, cache)
        }


        vertx.exceptionHandler {
            error -> logger.log(Level.SEVERE, "An error occurred while processing the request", error)
        }

        return vertx.createHttpServer(serverOptions)
            .requestHandler(mainRouter)
            .rxListen(serverConfig.port)
            .toCompletable()
    }

    private fun handleCommandSavepointForget(message: Message<String>, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<JobCommand, Void?>(
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
                    JobContext(cache.getFlinkJob(it.selector.resourceName()) ?: throw RuntimeException("Job not found"))
                )
            }
        )
    }

    private fun handleCommandSavepointTrigger(message: Message<String>, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<JobCommand, Void?>(
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
                    JobContext(cache.getFlinkJob(it.selector.resourceName()) ?: throw RuntimeException("Job not found"))
                )
            }
        )
    }

    private fun handleCommandJobUpdate(message: Message<String>, controller: Controller) {
        processCommandAndReplyToSender<JobCommand, Void?>(
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

    private fun handleCommandJobCreate(message: Message<String>, controller: Controller) {
        processCommandAndReplyToSender<JobCommand, Void?>(
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

    private fun handleCommandJobDelete(message: Message<String>, controller: Controller) {
        processCommandAndReplyToSender<JobCommand, Void?>(
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

    private fun handleCommandJobScale(message: Message<String>, controller: Controller) {
        processCommandAndReplyToSender<JobCommand, Void?>(
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

    private fun handleCommandJobStop(message: Message<String>, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<JobCommand, Void?>(
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
                    JobContext(cache.getFlinkJob(it.selector.resourceName()) ?: throw RuntimeException("Job not found"))
                )
            }
        )
    }

    private fun handleCommandJobStart(message: Message<String>, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<JobCommand, Void?>(
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
                    JobContext(cache.getFlinkJob(it.selector.resourceName()) ?: throw RuntimeException("Job not found"))
                )
            }
        )
    }

    private fun handleCommandDeploymentUpdate(message: Message<String>, controller: Controller) {
        processCommandAndReplyToSender<DeploymentCommand, Void?>(
            message,
            {
                json.deserialize(
                    it.body(), DeploymentCommand::class.java
                )
            },
            {
                controller.updateFlinkDeployment(
                    it.selector.namespace,
                    it.selector.deploymentName,
                    makeFlinkDeployment(controller, it.selector.namespace, it.selector.deploymentName, it.json)
                )
            }
        )
    }

    private fun handleCommandDeploymentCreate(message: Message<String>, controller: Controller) {
        processCommandAndReplyToSender<DeploymentCommand, Void?>(
            message,
            {
                json.deserialize(
                    it.body(), DeploymentCommand::class.java
                )
            },
            {
                controller.createFlinkDeployment(
                    it.selector.namespace,
                    it.selector.deploymentName,
                    makeFlinkDeployment(controller, it.selector.namespace, it.selector.deploymentName, it.json)
                )
            }
        )
    }

    private fun handleCommandDeploymentDelete(message: Message<String>, controller: Controller) {
        processCommandAndReplyToSender<DeploymentCommand, Void?>(
            message,
            {
                json.deserialize(
                    it.body(), DeploymentCommand::class.java
                )
            },
            {
                controller.deleteFlinkDeployment(
                    it.selector.namespace,
                    it.selector.deploymentName
                )
            }
        )
    }

    private fun handleCommandClusterUpdate(message: Message<String>, controller: Controller) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
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

    private fun handleCommandClusterCreate(message: Message<String>, controller: Controller) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
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

    private fun handleCommandClusterDelete(message: Message<String>, controller: Controller) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
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

    private fun handleCommandClusterScale(message: Message<String>, controller: Controller) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
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

    private fun handlerCommandClusterStop(message: Message<String>, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
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
                    ClusterContext(cache.getFlinkCluster(it.selector.resourceName()) ?: throw RuntimeException("Cluster not found"))
                )
            }
        )
    }

    private fun handleCommandClusterStart(message: Message<String>, controller: Controller, cache: Cache) {
        processCommandAndReplyToSender<ClusterCommand, Void?>(
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
                    ClusterContext(cache.getFlinkCluster(it.selector.resourceName()) ?: throw RuntimeException("Cluster not found"))
                )
            }
        )
    }

    private fun handleGetTaskManagerMetrics(routingContext: RoutingContext, controller: Controller, namespace: String) {
        processRequest(routingContext, Function { context ->
            controller.getTaskManagerMetrics(
                namespace, context.pathParam("clusterName"),
                TaskManagerId(context.pathParam("taskmanager"))
            )
        })
    }

    private fun handleGetTaskManagerDetails(routingContext: RoutingContext, controller: Controller, namespace: String) {
        processRequest(routingContext, Function { context ->
            controller.getTaskManagerDetails(
                namespace, context.pathParam("clusterName"),
                TaskManagerId(context.pathParam("taskmanager"))
            )
        })
    }

    private fun handleListTaskManagers(routingContext: RoutingContext, controller: Controller, namespace: String) {
        processRequest(routingContext, Function { context ->
            controller.getTaskManagersList(
                namespace, context.pathParam("clusterName")
            )
        })
    }

    private fun handleGetJobManagerMetrics(routingContext: RoutingContext, controller: Controller, namespace: String) {
        processRequest(routingContext, Function { context ->
            controller.getJobManagerMetrics(
                namespace, context.pathParam("clusterName")
            )
        })
    }

    private fun handleGetJobMetrics(routingContext: RoutingContext, controller: Controller, namespace: String, cache: Cache) {
        processRequest(routingContext, Function { context ->
            controller.getJobMetrics(
                namespace,
                context.pathParam("clusterName"),
                context.pathParam("jobName"),
                makeJobContext(cache, context.pathParam("clusterName") + "-" + context.pathParam("jobName"))
            )
        })
    }

    private fun handleGetJobDetails(routingContext: RoutingContext, controller: Controller, namespace: String, cache: Cache) {
        processRequest(routingContext, Function { context ->
            controller.getJobDetails(
                namespace,
                context.pathParam("clusterName"),
                context.pathParam("jobName"),
                makeJobContext(cache, context.pathParam("clusterName") + "-" + context.pathParam("jobName"))
            )
        })
    }

    private fun handleGetJobStatus(routingContext: RoutingContext, controller: Controller, namespace: String, cache: Cache) {
        processRequest(routingContext, Function { context ->
            controller.getFlinkJobStatus(
                namespace,
                context.pathParam("clusterName"),
                context.pathParam("jobName"),
                makeJobContext(cache, context.pathParam("clusterName") + "-" + context.pathParam("jobName"))
            )
        })
    }

    private fun handleListJobs(routingContext: RoutingContext, cache: Cache) {
        processRequest(routingContext, Function { context ->
            Result(ResultStatus.OK, cache.listJobNames().filter {
                it.startsWith(context.pathParam("clusterName") + "-")
            }.map {
                it.removePrefix(context.pathParam("clusterName") + "-")
            })
        })
    }

    private fun handleGetClusterStatus(routingContext: RoutingContext, controller: Controller, namespace: String, cache: Cache) {
        processRequest(routingContext, Function { context ->
            controller.getFlinkClusterStatus(
                namespace,
                context.pathParam("clusterName"),
                makeClusterContext(cache, context.pathParam("clusterName"))
            )
        })
    }

    private fun handleListClusters(routingContext: RoutingContext, cache: Cache) {
        processRequest(routingContext, Function {
            Result(ResultStatus.OK, cache.listClusterNames())
        })
    }

    private fun handleGetDeploymentStatus(routingContext: RoutingContext, controller: Controller, namespace: String, cache: Cache) {
        processRequest(routingContext, Function { context ->
            controller.getFlinkDeploymentStatus(
                namespace,
                context.pathParam("clusterName"),
                makeDeploymentContext(cache, context.pathParam("clusterName"))
            )
        })
    }

    private fun handleListDeployments(routingContext: RoutingContext, cache: Cache) {
        processRequest(routingContext, Function {
            Result(ResultStatus.OK, cache.listDeploymentNames())
        })
    }

    private fun handleDeploymentCommand(routingContext: RoutingContext, namespace: String, path: String, payloadSupplier: Function<RoutingContext, String>) {
        sendMessageAndWaitForReply(routingContext, namespace, path, BiFunction { context, namespace ->
            json.serialize(DeploymentCommand(
                DeploymentSelector(namespace = namespace, deploymentName = context.pathParam("deploymentName")), payloadSupplier.apply(context)
            ))
        })
    }

    private fun handleClusterCommand(routingContext: RoutingContext, namespace: String, path: String, payloadSupplier: Function<RoutingContext, String>) {
        sendMessageAndWaitForReply(routingContext, namespace, path, BiFunction { context, namespace ->
            json.serialize(ClusterCommand(
                ClusterSelector(namespace = namespace, clusterName = context.pathParam("clusterName")), payloadSupplier.apply(context)
            ))
        })
    }

    private fun handleJobCommand(routingContext: RoutingContext, namespace: String, path: String, payloadSupplier: Function<RoutingContext, String>) {
        sendMessageAndWaitForReply(routingContext, namespace, path, BiFunction { context, namespace ->
            json.serialize(JobCommand(
                JobSelector(namespace = namespace, clusterName = context.pathParam("clusterName"), jobName = context.pathParam("jobName")), payloadSupplier.apply(context)
            ))
        })
    }

    private fun makeDeploymentContext(cache: Cache, resourceName: String) =
        DeploymentContext(cache.getFlinkDeployment(resourceName) ?: throw RuntimeException("Deployment not found"))

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
            logger.log(Level.WARNING, "HTTPS not enabled!")
        }

        return serverOptions
    }

    private fun makeError(error: Throwable) = "{\"status\":\"FAILURE\",\"error\":\"${error.message}\"}"

    private fun makeFlinkDeployment(controller: Controller, namespace: String, clusterName: String, json: String): V1FlinkDeployment {
        return controller.makeFlinkDeployment(namespace, clusterName, Resource.parseV1FlinkDeploymentSpec(json))
    }

    private fun makeFlinkCluster(controller: Controller, namespace: String, clusterName: String, json: String): V1FlinkCluster {
        return controller.makeFlinkCluster(namespace, clusterName, Resource.parseV1FlinkClusterSpec(json))
    }

    private fun makeFlinkJob(controller: Controller, namespace: String, clusterName: String, jobName: String, json: String): V1FlinkJob {
        return controller.makeFlinkJob(namespace, clusterName, jobName, Resource.parseV1FlinkJobSpec(json))
    }

    private fun <R> processRequest(context: RoutingContext, handler: Function<RoutingContext, Result<R>>) {
        Single.just(context)
            .map {
                val result = handler.apply(context)

                if (result.isSuccessful()) {
                    json.serialize(Result(ResultStatus.OK, result.output?.let { json.serialize(it) }))
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
                logger.log(Level.WARNING, "Can't process request", it)
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
                logger.log(Level.WARNING, "Can't process request", it)
            }
            .subscribe()
    }

    private fun <T, R> processCommandAndReplyToSender(
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
                logger.log(Level.SEVERE, "Can't process command [address=${message.address()}]", it)
            }
            .onErrorReturn {
                makeError(it)
            }
            .doOnSuccess {
                message.reply(it)
            }
            .doOnError {
                logger.log(Level.SEVERE, "Can't send response [address=${message.address()}]", it)
            }
            .subscribe()
    }
}
