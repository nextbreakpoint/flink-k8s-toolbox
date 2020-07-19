package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import org.apache.log4j.Logger

class OnStarting(logger: Logger) : Task(logger) {
    override fun execute(context: TaskContext) {
        if (context.hasBeenDeleted()) {
            context.setDeleteResources(true)
            context.resetManualAction()
            context.setClusterStatus(ClusterStatus.Cancelling)

            return
        }

        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TASK_TIMEOUT) {
            logger.error("Cluster not started after $seconds seconds")

            context.resetSavepointRequest()
            context.setClusterStatus(ClusterStatus.Failed)

            return
        }

        val jobmanagerServiceExists = context.doesJobManagerServiceExists()
        val jobmanagerStatefuleSetExists = context.doesJobManagerStatefulSetExists()
        val taskmanagerStatefuleSetExists = context.doesTaskManagerStatefulSetExists()

        if (!jobmanagerServiceExists) {
            val serviceResult = context.createJobManagerService(context.clusterSelector)

            if (serviceResult.isSuccessful()) {
                logger.info("JobManager service created: ${serviceResult.output}")
            }
        }

        if (!jobmanagerStatefuleSetExists) {
            val statefulSetResult = context.createJobManagerStatefulSet(context.clusterSelector)

            if (statefulSetResult.isSuccessful()) {
                logger.info("JobManager statefulset created: ${statefulSetResult.output}")
            }
        }

        if (!taskmanagerStatefuleSetExists) {
            val statefulSetResult = context.createTaskManagerStatefulSet(context.clusterSelector)

            if (statefulSetResult.isSuccessful()) {
                logger.info("TaskManager statefulset created: ${statefulSetResult.output}")
            }
        }

        if (jobmanagerServiceExists && jobmanagerStatefuleSetExists && taskmanagerStatefuleSetExists) {
            val clusterScaling = context.getClusterScale()

            val jobmanagerReplicas = context.getJobManagerReplicas()
            val taskmanagerReplicas = context.getTaskManagerReplicas()

            if (jobmanagerReplicas != 1 || taskmanagerReplicas != clusterScaling.taskManagers) {
                logger.info("Restating pods...")

                context.restartPods(context.clusterSelector, clusterScaling)

                return
            }

            if (!context.isBootstrapPresent()) {
                logger.info("Cluster running")

                context.resetSavepointRequest()
                context.setClusterStatus(ClusterStatus.Running)

                return
            }

            if (context.doesBootstrapJobExists()) {
                val jobRunningResult = context.isJobRunning(context.clusterSelector)

                if (!jobRunningResult.output) {
                    return
                }

                logger.info("Job running")

                context.resetSavepointRequest()
                context.setClusterStatus(ClusterStatus.Running)

                return
            } else {
                val clusterReadyResult = context.isClusterReady(context.clusterSelector, clusterScaling)

                if (!clusterReadyResult.output) {
                    return
                }

                logger.info("Cluster ready")

                val removeJarResult = context.removeJar(context.clusterSelector)

                if (!removeJarResult.isSuccessful()) {
                    return
                }

                logger.info("JARs removed")

                val bootstrapResult = context.createBootstrapJob(context.clusterSelector)

                if (bootstrapResult.isSuccessful()) {
                    logger.info("Bootstrap job created: ${bootstrapResult.output}")
                }

                return
            }
        }
    }
}