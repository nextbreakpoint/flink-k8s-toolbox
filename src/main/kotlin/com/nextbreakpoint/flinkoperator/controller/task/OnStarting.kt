package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import org.apache.log4j.Logger

class OnStarting(logger: Logger) : Task(logger) {
    override fun execute(context: TaskContext) {
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
            val serviceResult = context.createJobManagerService(context.clusterId)

            if (serviceResult.isCompleted()) {
                logger.info("JobManager service created: ${serviceResult.output}")
            }
        }

        if (!jobmanagerStatefuleSetExists) {
            val statefulSetResult = context.createJobManagerStatefulSet(context.clusterId)

            if (statefulSetResult.isCompleted()) {
                logger.info("JobManager statefulset created: ${statefulSetResult.output}")
            }
        }

        if (!taskmanagerStatefuleSetExists) {
            val statefulSetResult = context.createTaskManagerStatefulSet(context.clusterId)

            if (statefulSetResult.isCompleted()) {
                logger.info("TaskManager statefulset created: ${statefulSetResult.output}")
            }
        }

        if (jobmanagerServiceExists && jobmanagerStatefuleSetExists && taskmanagerStatefuleSetExists) {
            val bootstrapExists = context.doesBootstrapExists()

            val options = context.getClusterScale()

            val jobmanagerReplicas = context.getJobManagerReplicas()
            val taskmanagerReplicas = context.getTaskManagerReplicas()

            if (jobmanagerReplicas != 1 || taskmanagerReplicas != options.taskManagers) {
                logger.info("Restating pods...")

                context.restartPods(context.clusterId, options)

                return
            }

            if (!context.isBootstrapPresent()) {
                logger.info("Cluster running")

                context.resetSavepointRequest()
                context.setClusterStatus(ClusterStatus.Running)

                return
            }

            if (bootstrapExists) {
                val jobRunningResult = context.isJobRunning(context.clusterId)

                if (!jobRunningResult.isCompleted()) {
                    return
                }

                logger.info("Job running")

                context.resetSavepointRequest()
                context.setClusterStatus(ClusterStatus.Running)

                return
            } else {
                val options = context.getClusterScale()

                val clusterReadyResult = context.isClusterReady(context.clusterId, options)

                if (!clusterReadyResult.isCompleted()) {
                    return
                }

                logger.info("Cluster ready")

                val removeJarResult = context.removeJar(context.clusterId)

                if (!removeJarResult.isCompleted()) {
                    return
                }

                logger.info("JARs removed")

                val bootstrapResult = context.createBootstrapJob(context.clusterId)

                if (bootstrapResult.isCompleted()) {
                    logger.info("Bootstrap job created: ${bootstrapResult.output}")
                }

                return
            }
        }
    }
}