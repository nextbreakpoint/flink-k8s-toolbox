package com.nextbreakpoint.flinkoperator.controller.command

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.utils.FlinkServerUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkclient.model.ClusterOverviewWithVersion
import com.nextbreakpoint.flinkclient.model.JarListInfo
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.OperatorParameters
import org.apache.log4j.Logger

class JarRun(flinkOptions: FlinkOptions) : OperatorCommand<V1FlinkCluster, Void?>(flinkOptions) {
    private val logger = Logger.getLogger(JarRun::class.simpleName)

    override fun execute(clusterId: ClusterId, params: V1FlinkCluster): Result<Void?> {
        try {
            val flinkApi = FlinkServerUtils.find(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkApi.jobs.jobs.filter {
                    jobIdWithStatus -> jobIdWithStatus.status.value.equals("RUNNING")
            }.map {
                it.id
            }.toList()

            if (runningJobs.isNotEmpty()) {
                logger.warn("Expected no job running in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }

            val listJarsResponse = flinkApi.listJarsCall(null, null).execute()

            if (!listJarsResponse.isSuccessful) {
                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }

            listJarsResponse.body().use {
                val jarList = Gson().fromJson(it.source().readUtf8Line(), JarListInfo::class.java)

                val jarFile = jarList.files.toList().sortedByDescending { it.uploaded }.firstOrNull()

                if (jarFile != null) {
                    val overviewResponse = flinkApi.getOverviewCall(null, null).execute()

                    if (!overviewResponse.isSuccessful) {
                        return Result(
                            ResultStatus.FAILED,
                            null
                        )
                    }

                    overviewResponse.body().use {
                        val overview = Gson().fromJson(it.source().readUtf8Line(), ClusterOverviewWithVersion::class.java)

                        if (overview.jobsRunning > 0) {
                            return Result(
                                ResultStatus.FAILED,
                                null
                            )
                        }

                        val savepointPath = OperatorParameters.getSavepointPath(params)

                        val runJarResponse = flinkApi.runJarCall(
                            jarFile.id,
                            false,
                            savepointPath,
                            params.spec.flinkJob.arguments.joinToString(separator = " "),
                            null,
                            params.spec.flinkJob.className,
                            params.spec.flinkJob.parallelism,
                            null,
                            null
                        ).execute()

                        if (!runJarResponse.isSuccessful) {
                            return Result(
                                ResultStatus.FAILED,
                                null
                            )
                        }

                        runJarResponse.body().use {
                            logger.debug("Job started: ${it.source().readUtf8Line()}")
                        }

                        return Result(
                            ResultStatus.SUCCESS,
                            null
                        )
                    }
                } else {
                    logger.warn("Can't find any JAR file in cluster ${clusterId.name}")

                    return Result(
                        ResultStatus.AWAIT,
                        null
                    )
                }
            }
        } catch (e : Exception) {
            logger.warn("Can't get the list of JAR files of cluster ${clusterId.name}")

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}