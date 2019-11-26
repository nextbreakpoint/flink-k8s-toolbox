package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient

abstract class Operation<T, R>(val flinkOptions: FlinkOptions, val flinkClient: FlinkClient, val kubeClient: KubeClient) {
    abstract fun execute(clusterId: ClusterId, params: T): OperationResult<R>

    fun updateDigests(flinkCluster: V1FlinkCluster) {
        val actualJobManagerDigest = ClusterResource.computeDigest(flinkCluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(flinkCluster.spec?.taskManager)
        val actualRuntimeDigest = ClusterResource.computeDigest(flinkCluster.spec?.runtime)
        val actualBootstrapDigest = ClusterResource.computeDigest(flinkCluster.spec?.bootstrap)

        Status.setJobManagerDigest(flinkCluster, actualJobManagerDigest)
        Status.setTaskManagerDigest(flinkCluster, actualTaskManagerDigest)
        Status.setRuntimeDigest(flinkCluster, actualRuntimeDigest)
        Status.setBootstrapDigest(flinkCluster, actualBootstrapDigest)
    }
}