package com.nextbreakpoint.flink.k8s.operator

import com.nextbreakpoint.flink.common.ServerConfig
import com.nextbreakpoint.flink.k8s.common.FlinkDeploymentStatus
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.operator.core.Cache
import com.nextbreakpoint.flink.k8s.operator.core.DeploymentManager
import com.nextbreakpoint.flink.k8s.operator.core.JobManager
import com.nextbreakpoint.flink.k8s.operator.core.OperatorController
import com.nextbreakpoint.flink.k8s.operator.core.SupervisorManager
import java.util.logging.Level
import java.util.logging.Logger

class Operator(
    private val controller: Controller,
    private val cache: Cache,
    private val taskTimeout: Long,
    private val pollingInterval: Long,
    private val serverConfig: ServerConfig,
) {
    companion object {
        private val logger = Logger.getLogger(Operator::class.simpleName)

        fun create(controller: Controller, cache: Cache, taskTimeout: Long, pollingInterval: Long, serverConfig: ServerConfig): Operator {
            return Operator(controller, cache, taskTimeout, pollingInterval, serverConfig)
        }
    }

    fun reconcile() {
        cache.getFlinkJobs().forEach { reconcile(it) }

        cache.getFlinkClusters().forEach { reconcile(it) }

        cache.getFlinkDeployments().forEach { reconcile(it) }
    }

    private fun reconcile(cluster: V1FlinkCluster) {
        try {
            val clusterName = getName(cluster)

            val logger = Logger.getLogger(getLoggerName(clusterName))

            val operatorController = OperatorController(cache.namespace, controller, serverConfig)

            val supervisorManager = SupervisorManager(logger, cache, operatorController, cluster)

            supervisorManager.reconcile()
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "Error occurred while reconciling resources", e)
        }
    }

    private fun reconcile(job: V1FlinkJob) {
        try {
            val resourceName = getName(job)

            val operatorController = OperatorController(cache.namespace, controller, serverConfig)

            val jobManager = JobManager(logger, cache, operatorController, job)

            val statusTimestamp = FlinkJobStatus.getStatusTimestamp(job)

            jobManager.reconcile()

            val newStatusTimestamp = FlinkJobStatus.getStatusTimestamp(job)

            if (statusTimestamp != newStatusTimestamp) {
                logger.log(Level.FINE, "Updating status: job $resourceName")
                operatorController.updateStatus(cache.namespace, resourceName, job)
            }
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "Error occurred while reconciling resources", e)
        }
    }

    private fun reconcile(deployment: V1FlinkDeployment) {
        try {
            val resourceName = getName(deployment)

            val statusTimestamp = FlinkDeploymentStatus.getStatusTimestamp(deployment)

            val operatorController = OperatorController(cache.namespace, controller, serverConfig)

            val hasFinalizer = operatorController.hasFinalizer(deployment)

            val deploymentManager = DeploymentManager(logger, cache, operatorController, deployment)

            deploymentManager.reconcile()

            val newStatusTimestamp = FlinkDeploymentStatus.getStatusTimestamp(deployment)

            if (statusTimestamp != newStatusTimestamp) {
                logger.log(Level.FINE, "Updating status: deployment $resourceName")
                operatorController.updateStatus(cache.namespace, resourceName, deployment)
            }

            val newHasFinalizer = operatorController.hasFinalizer(deployment)

            if (hasFinalizer != newHasFinalizer) {
                logger.log(Level.FINE, "Updating finalizers: deployment $resourceName")
                operatorController.updateFinalizers(cache.namespace, resourceName, deployment)
            }
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "Error occurred while reconciling resources", e)
        }
    }

    private fun getName(job: V1FlinkJob) =
        job.metadata?.name ?: throw RuntimeException("Metadata name is null")

    private fun getName(cluster: V1FlinkCluster) =
        cluster.metadata?.name ?: throw RuntimeException("Metadata name is null")

    private fun getName(deployment: V1FlinkDeployment) =
        deployment.metadata?.name ?: throw RuntimeException("Metadata name is null")

    private fun getLoggerName(clusterName: String) =
        Operator::class.simpleName + " $clusterName"
}
