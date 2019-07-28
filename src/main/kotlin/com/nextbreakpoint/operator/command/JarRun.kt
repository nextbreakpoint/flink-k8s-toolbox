package com.nextbreakpoint.operator.command

import com.google.gson.Gson
import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.flinkclient.model.ClusterOverviewWithVersion
import com.nextbreakpoint.flinkclient.model.JarListInfo
import com.nextbreakpoint.model.V1FlinkCluster
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class JarRun(flinkOptions: FlinkOptions) : OperatorCommand<V1FlinkCluster, Void?>(flinkOptions) {
    private val logger = Logger.getLogger(JarRun::class.simpleName)

    override fun execute(clusterId: ClusterId, params: V1FlinkCluster): Result<Void?> {
        try {
            val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

            val listJarsResponse = flinkApi.listJarsCall(null, null).execute()

            if (!listJarsResponse.isSuccessful) {
                return Result(ResultStatus.FAILED, null)
            }

            val jarList = Gson().fromJson(listJarsResponse.body().source().readUtf8Line(), JarListInfo::class.java)

            val jarFile = jarList.files.toList().sortedByDescending { it.uploaded }.firstOrNull()

            if (jarFile != null) {
                val overviewResponse = flinkApi.getOverviewCall(null, null).execute()

                if (!overviewResponse.isSuccessful) {
                    return Result(ResultStatus.FAILED,null)
                }

                overviewResponse.body().use {
                    val overview = Gson().fromJson(it.source().readUtf8Line(), ClusterOverviewWithVersion::class.java)

                    if (overview.jobsRunning > 0) {
                        return Result(ResultStatus.FAILED, null)
                    }

                    val runJarResponse = flinkApi.runJarCall(
                        jarFile.id,
                        false,
                        OperatorAnnotations.getSavepointPath(params) ?: params.spec.flinkJob.savepoint,
                        params.spec.flinkJob.arguments.joinToString(separator = " "),
                        null,
                        params.spec.flinkJob.className,
                        params.spec.flinkJob.parallelism,
                        null,
                        null
                    ).execute()

                    if (!runJarResponse.isSuccessful) {
                        return Result(ResultStatus.FAILED, null)
                    }

                    logger.debug("Job started: ${Gson().toJson(listJarsResponse)}")

                    return Result(ResultStatus.SUCCESS, null)
                }
            } else {
//                logger.warn("Can't find any JAR files in cluster ${clusterId.name}")

                return Result(ResultStatus.AWAIT, null)
            }
        } catch (e : Exception) {
            logger.error("Can't get the list of JAR files of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }
}