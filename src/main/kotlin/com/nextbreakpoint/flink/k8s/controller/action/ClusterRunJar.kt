package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.RunJarOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class ClusterRunJar(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<RunJarOptions, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterRunJar::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: RunJarOptions): Result<String> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val result = flinkClient.runJar(address, params.jarFileId, params.className, params.parallelism, params.savepointPath, params.arguments)

            Result(
                ResultStatus.OK,
                result.jobid
            )
        } catch (e : Exception) {
            logger.error("Can't run JAR file", e)

            Result(
                ResultStatus.ERROR,
                ""
            )
        }
    }
}