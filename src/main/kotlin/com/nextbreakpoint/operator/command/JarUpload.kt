package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand
import com.nextbreakpoint.operator.resources.ClusterResources
import org.apache.log4j.Logger

class JarUpload(flinkOptions: FlinkOptions) : OperatorCommand<ClusterResources, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(JarUpload::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterResources): Result<Void?> {
        try {
            val jobs = Kubernetes.batchApi.listNamespacedJob(
                clusterId.namespace,
                null,
                null,
                null,
                null,
                "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator",
                null,
                null,
                30,
                null
            )

            if (jobs.items.isEmpty()) {
                logger.info("Creating upload Job of cluster ${clusterId.name}...")

                val jobOut = Kubernetes.batchApi.createNamespacedJob(
                    clusterId.namespace,
                    params.jarUploadJob,
                    null,
                    null,
                    null
                )

                logger.info("Upload job of cluster ${clusterId.name} created with name ${jobOut.metadata.name}")

                return Result(ResultStatus.SUCCESS, null)
            } else {
                logger.warn("Upload job of cluster ${clusterId.name} already exists")

                return Result(ResultStatus.FAILED, null)
            }
        } catch (e : Exception) {
            logger.error("Can't create upload job of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }
}