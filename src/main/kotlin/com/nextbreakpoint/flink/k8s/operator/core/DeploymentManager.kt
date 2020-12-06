package com.nextbreakpoint.flink.k8s.operator.core

import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.k8s.common.FlinkDeploymentStatus
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterDigest
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeploymentDigest
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeploymentJobDigest
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeploymentJobSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobDigest
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec
import org.apache.log4j.Logger

class DeploymentManager(
    private val logger: Logger,
    private val cache: Cache,
    private val controller: OperatorController,
    private val deployment: V1FlinkDeployment
) {
    fun reconcile() {
        FlinkDeploymentStatus.setResourceStatus(deployment, ResourceStatus.Updated)

        if (deployment.metadata?.deletionTimestamp != null) {
            if (hasDeploymentBeenTerminated()) {
                logger.info("Remove finalizer: deployment ${deployment.metadata.name}")
                controller.removeFinalizer(deployment)
            }
        } else {
            if (!controller.hasFinalizer(deployment)) {
                logger.info("Add finalizer: deployment ${deployment.metadata.name}")
                controller.addFinalizer(deployment)
            } else {
                if (hasDeploymentBeenInitialised()) {
                    updateDeployment()
                }
            }
        }
    }

    private fun hasDeploymentBeenInitialised() : Boolean {
        if (deployment.status?.digest == null) {
            FlinkDeploymentStatus.setDeploymentDigest(deployment, makeDeploymentDigest())

            return false
        }

        return true
    }

    private fun updateDeployment() {
        val clusterName = getName(deployment)

        val deployedCluster = cache.getFlinkCluster(clusterName)

        val deployedJobs = getDeployedJobs(clusterName)

        val declaredJobs = getDeclaredJobs()

        val deploymentDigest = makeDeploymentDigest()

        if (deployedCluster == null) {
            val result = controller.createCluster(cache.namespace, clusterName, deployment.spec.cluster)

            if (!result.isSuccessful()) {
                logger.error("Can't create cluster: $clusterName")
            }

            logger.info("Cluster $clusterName created")

            FlinkDeploymentStatus.setDeploymentDigest(deployment, deploymentDigest)

            FlinkDeploymentStatus.setResourceStatus(deployment, ResourceStatus.Updating)
        }

        deployedJobs.entries.forEach {
            val jobName = it.key
            val declaredJobSpec = declaredJobs[jobName]

            if (declaredJobSpec == null) {
                val result = controller.deleteJob(cache.namespace, clusterName, jobName)

                if (!result.isSuccessful()) {
                    logger.error("Can't delete job: $clusterName-$jobName")
                }

                logger.info("Job $clusterName-$jobName deleted")

                FlinkDeploymentStatus.setDeploymentDigest(deployment, deploymentDigest)

                FlinkDeploymentStatus.setResourceStatus(deployment, ResourceStatus.Updating)
            } else if (it.value == null) {
                val result = controller.createJob(cache.namespace, clusterName, jobName, declaredJobSpec)

                if (!result.isSuccessful()) {
                    logger.error("Can't create job: $clusterName-$jobName")
                }

                logger.info("Job $clusterName-$jobName created")

                FlinkDeploymentStatus.setDeploymentDigest(deployment, deploymentDigest)

                FlinkDeploymentStatus.setResourceStatus(deployment, ResourceStatus.Updating)
            }
        }

        declaredJobs.entries.forEach {
            val jobName = it.key
            val declaredJobSpec = it.value

            if (!deployedJobs.containsKey(jobName)) {
                val result = controller.createJob(cache.namespace, clusterName, jobName, declaredJobSpec)

                if (!result.isSuccessful()) {
                    logger.error("Can't create job: $clusterName-$jobName")
                }

                logger.info("Job $clusterName-$jobName created")

                FlinkDeploymentStatus.setDeploymentDigest(deployment, deploymentDigest)

                FlinkDeploymentStatus.setResourceStatus(deployment, ResourceStatus.Updating)
            }
        }

        if (deployedCluster != null) {
            val deployedClusterSpec = deployedCluster.spec
            val declaredClusterSpec = deployment.spec.cluster

            if (declaredClusterSpec != null) {
                val deployedClusterDigest = deployment.status.digest.cluster
                val declaredClusterDigest = deploymentDigest.cluster

                if (isClusterSpecChanged(deployedClusterDigest, declaredClusterDigest) || areScaleLimitsChanged(deployedClusterSpec, declaredClusterSpec)) {
                    deployedCluster.spec.runtime = declaredClusterSpec.runtime
                    deployedCluster.spec.jobManager = declaredClusterSpec.jobManager
                    deployedCluster.spec.taskManager = declaredClusterSpec.taskManager
                    deployedCluster.spec.supervisor = declaredClusterSpec.supervisor
                    deployedCluster.spec.minTaskManagers = declaredClusterSpec.minTaskManagers
                    deployedCluster.spec.maxTaskManagers = declaredClusterSpec.maxTaskManagers

                    val result = controller.updateCluster(cache.namespace, clusterName, deployedCluster)

                    if (!result.isSuccessful()) {
                        logger.error("Can't update cluster: $clusterName")
                    }

                    logger.info("Cluster $clusterName updated")

                    FlinkDeploymentStatus.setDeploymentDigest(deployment, deploymentDigest)

                    FlinkDeploymentStatus.setResourceStatus(deployment, ResourceStatus.Updating)
                }
            }
        }

        val deployedJobDigests = deployment.status.digest.jobs.map { it.name to it.job }.toMap()
        val declaredJobDigests = deploymentDigest.jobs.map { it.name to it.job }.toMap()

        deployedJobs.entries.map {
            val jobName = it.key
            val deployedJob = it.value

            if (deployedJob != null) {
                val deployedJobSpec = deployedJob.spec
                val declaredJobSpec = declaredJobs[jobName]

                if (declaredJobSpec != null) {
                    val deployedJobDigest = deployedJobDigests[jobName]
                    val declaredJobDigest = declaredJobDigests[jobName]

                    if (deployedJobDigest != null && declaredJobDigest != null && isFlinkJobSpecChanged(deployedJobDigest, declaredJobDigest) || areScaleLimitsChanged(deployedJobSpec, declaredJobSpec)) {
                        deployedJob.spec.bootstrap = declaredJobSpec.bootstrap
                        deployedJob.spec.savepoint = declaredJobSpec.savepoint
                        deployedJob.spec.minJobParallelism = declaredJobSpec.minJobParallelism
                        deployedJob.spec.maxJobParallelism = declaredJobSpec.maxJobParallelism

                        val result = controller.updateJob(cache.namespace, clusterName, jobName, deployedJob)

                        if (!result.isSuccessful()) {
                            logger.error("Can't update job: $clusterName-$jobName")
                        }

                        logger.info("Job $clusterName-$jobName updated")

                        FlinkDeploymentStatus.setDeploymentDigest(deployment, deploymentDigest)

                        FlinkDeploymentStatus.setResourceStatus(deployment, ResourceStatus.Updating)
                    }
                }
            }
        }
    }

    private fun hasDeploymentBeenTerminated(): Boolean {
        val clusterName = getName(deployment)

        val deployedCluster = cache.getFlinkCluster(clusterName)

        val deploymentDigest = makeDeploymentDigest()

        if (deployedCluster != null && deployedCluster.metadata.deletionTimestamp == null) {
            val result = controller.deleteCluster(cache.namespace, clusterName)

            if (!result.isSuccessful()) {
                logger.error("Can't delete cluster: $clusterName")
            }

            logger.info("Cluster $clusterName deleted")

            FlinkDeploymentStatus.setDeploymentDigest(deployment, deploymentDigest)

            FlinkDeploymentStatus.setResourceStatus(deployment, ResourceStatus.Updating)
        }

        val deployedJobs = getDeployedJobs(clusterName)

        deployedJobs.entries.forEach {
            val jobName = it.key
            val deployedJob = it.value

            if (deployedJob != null && deployedJob.metadata.deletionTimestamp == null) {
                val result = controller.deleteJob(cache.namespace, clusterName, jobName)

                if (!result.isSuccessful()) {
                    logger.error("Can't delete job: $clusterName-$jobName")
                }

                logger.info("Job $clusterName-$jobName deleted")

                FlinkDeploymentStatus.setDeploymentDigest(deployment, deploymentDigest)

                FlinkDeploymentStatus.setResourceStatus(deployment, ResourceStatus.Updating)
            }
        }

        if (deployedCluster != null || deployedJobs.any { it.value != null }) {
            logger.info("Deployment deleted. Awaiting deployment termination...")
            return false
        } else if (controller.timeSinceLastUpdateInSeconds(deployment) > 30) {
            logger.info("Cluster and jobs have been deleted. Deployment terminated")
            return true
        }

        return false
    }

    private fun makeDeploymentDigest(): V1FlinkDeploymentDigest {
        return V1FlinkDeploymentDigest.builder()
            .withCluster(computeDigest(deployment.spec.cluster))
            .withJobs(makeFlinkDeploymentJobDigests())
            .build()
    }

    private fun makeFlinkDeploymentJobDigests() =
        deployment.spec.jobs?.map { makeDeploymentJobDigest(it) }

    private fun makeDeploymentJobDigest(jobSpec: V1FlinkDeploymentJobSpec) =
        V1FlinkDeploymentJobDigest.builder()
            .withJob(computeDigest(jobSpec.spec))
            .withName(jobSpec.name)
            .build()

    private fun isFlinkJobSpecChanged(deployedJobDigest: V1FlinkJobDigest, declaredJobDigest: V1FlinkJobDigest): Boolean {
        return deployedJobDigest.bootstrap != declaredJobDigest.bootstrap ||
               deployedJobDigest.savepoint != declaredJobDigest.savepoint
    }

    private fun isClusterSpecChanged(deployedClusterDigest: V1FlinkClusterDigest, declaredClusterDigest: V1FlinkClusterDigest): Boolean {
        return deployedClusterDigest.runtime != declaredClusterDigest.runtime ||
               deployedClusterDigest.jobManager != declaredClusterDigest.jobManager ||
               deployedClusterDigest.taskManager != declaredClusterDigest.taskManager ||
               deployedClusterDigest.supervisor != declaredClusterDigest.supervisor
    }

    private fun areScaleLimitsChanged(deployedJobSpec: V1FlinkJobSpec, declaredJobSpec: V1FlinkJobSpec) =
        deployedJobSpec.minJobParallelism != declaredJobSpec.minJobParallelism ||
        deployedJobSpec.maxJobParallelism != declaredJobSpec.maxJobParallelism

    private fun areScaleLimitsChanged(deployedClusterSpec: V1FlinkClusterSpec, declaredClusterSpec: V1FlinkClusterSpec) =
        deployedClusterSpec.minTaskManagers != declaredClusterSpec.minTaskManagers ||
        deployedClusterSpec.maxTaskManagers != declaredClusterSpec.maxTaskManagers

    private fun computeDigest(clusterSpec: V1FlinkClusterSpec): V1FlinkClusterDigest {
        return V1FlinkClusterDigest.builder()
            .withRuntime(Resource.computeDigest(clusterSpec.runtime))
            .withJobManager(Resource.computeDigest(clusterSpec.jobManager))
            .withTaskManager(Resource.computeDigest(clusterSpec.taskManager))
            .withSupervisor(Resource.computeDigest(clusterSpec.supervisor))
            .build()
    }

    private fun computeDigest(clusterSpec: V1FlinkJobSpec): V1FlinkJobDigest {
        return V1FlinkJobDigest.builder()
            .withBootstrap(Resource.computeDigest(clusterSpec.bootstrap))
            .withSavepoint(Resource.computeDigest(clusterSpec.savepoint))
            .build()
    }

    private fun getDeployedJobs(clusterName: String) =
        deployment.status?.digest?.jobs?.map {
            it.name to cache.getFlinkJob("$clusterName-${it.name}")
        }?.toMap().orEmpty()

    private fun getDeclaredJobs() =
        deployment.spec.jobs.orEmpty().map { it.name to it.spec }.toMap()

    private fun getName(deployment: V1FlinkDeployment) =
        deployment.metadata?.name ?: throw RuntimeException("Metadata name is null")
}
