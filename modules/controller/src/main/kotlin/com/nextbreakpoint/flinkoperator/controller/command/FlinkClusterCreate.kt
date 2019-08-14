package com.nextbreakpoint.flinkoperator.controller.command

import com.google.gson.GsonBuilder
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.crd.DateTimeSerializer
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger
import org.joda.time.DateTime

class FlinkClusterCreate(flinkOptions: FlinkOptions) : OperatorCommand<V1FlinkCluster, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(FlinkClusterCreate::class.simpleName)

        private val gson = GsonBuilder().registerTypeAdapter(DateTime::class.java,
            DateTimeSerializer()
        ).create()
    }

    override fun execute(clusterId: ClusterId, params: V1FlinkCluster): Result<Void?> {
        try {
            val flinkCluster = V1FlinkCluster()
                .apiVersion("nextbreakpoint.com/v1")
                .kind("FlinkCluster")
                .metadata(params.metadata)
                .spec(params.spec)

            val response = KubernetesUtils.objectApi.createNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                flinkCluster.metadata.namespace,
                "flinkclusters",
                gson.fromJson(gson.toJson(flinkCluster), Map::class.java /* oh boy, it works with map but not json or pojo !!! */),
                null
            )

            if (response.statusCode == 201) {
                logger.info("Custom object created ${flinkCluster.metadata.name}")

                return Result(
                    ResultStatus.SUCCESS,
                    null
                )
            } else {
                logger.error("Can't create custom object ${flinkCluster.metadata.name}")

                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }
        } catch (e : Exception) {
            logger.error("Can't create cluster resource ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}