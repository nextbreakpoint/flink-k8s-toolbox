package com.nextbreakpoint.operator.command

import com.google.gson.Gson
import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.flinkclient.model.JarListInfo
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class JarIsReady(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    private val logger = Logger.getLogger(JarIsReady::class.simpleName)

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

            val response = flinkApi.listJarsCall(null, null).execute()

            if (!response.isSuccessful) {
                return Result(ResultStatus.FAILED, null)
            }

            response.body().use {
                val jarList = Gson().fromJson(it.source().readUtf8Line(), JarListInfo::class.java)

                if (jarList.files.size > 0) {
                    return Result(ResultStatus.SUCCESS, null)
                } else {
                    return Result(ResultStatus.AWAIT, null)
                }
            }
        } catch (e : Exception) {
            logger.error("Can't get the list of JAR files of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }
}