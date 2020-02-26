package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.CacheAdapter
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import io.kubernetes.client.JSON

class ClusterGetStatus(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val adapter: CacheAdapter) : Operation<Void?, String>(flinkOptions, flinkClient, kubeClient) {
    override fun execute(clusterId: ClusterId, params: Void?): OperationResult<String> {
        return OperationResult(OperationStatus.COMPLETED, JSON().serialize(adapter.getStatus()))
    }
}