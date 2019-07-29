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

class JarRemove(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(JarRemove::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

            val response = flinkApi.listJarsCall(null, null).execute()

            if (!response.isSuccessful) {
                return Result(ResultStatus.FAILED, null)
            }

            response.body().use {
                val jarList = Gson().fromJson(it.source().readUtf8Line(), JarListInfo::class.java)

                jarList.files.forEach {
                    flinkApi.deleteJar(it.id)
                }

                return Result(ResultStatus.SUCCESS, null)
            }
        } catch (e : Exception) {
            logger.error("Can't remove JAR files of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }
}