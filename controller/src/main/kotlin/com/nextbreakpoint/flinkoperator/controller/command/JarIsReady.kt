package com.nextbreakpoint.flinkoperator.controller.command

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.utils.FlinkServerUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkclient.model.JarListInfo
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class JarIsReady(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(JarIsReady::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val flinkApi = FlinkServerUtils.find(flinkOptions, clusterId.namespace, clusterId.name)

            val response = flinkApi.listJarsCall(null, null).execute()

            if (!response.isSuccessful) {
                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }

            response.body().use {
                val jarList = Gson().fromJson(it.source().readUtf8Line(), JarListInfo::class.java)

                if (jarList.files.isNotEmpty()) {
                    return Result(
                        ResultStatus.SUCCESS,
                        null
                    )
                } else {
                    return Result(
                        ResultStatus.AWAIT,
                        null
                    )
                }
            }
        } catch (e : Exception) {
            logger.error("Can't get JAR files of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}