package com.nextbreakpoint.operator.command

import com.google.gson.Gson
import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.flinkclient.model.ClusterOverviewWithVersion
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class ClusterIsReady(flinkOptions : FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(ClusterIsReady::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

            val response = flinkApi.getOverviewCall(null, null).execute()

            if (!response.isSuccessful) {
                return Result(ResultStatus.FAILED, null)
            }

            response.body().use {
                val overview = Gson().fromJson(it.source().readUtf8Line(), ClusterOverviewWithVersion::class.java)

                if (overview.slotsAvailable > 0 && overview.taskmanagers > 0) {
                    return Result(ResultStatus.SUCCESS, null)
                } else {
                    return Result(ResultStatus.AWAIT, null)
                }
            }
        } catch (e : Exception) {
//            logger.error("Can't get overview of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }
}