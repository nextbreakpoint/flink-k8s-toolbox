package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterJobSpec
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder

object JobResourcesDefaultFactory : JobResourcesFactory {
    override fun createJob(
        clusterSelector: ResourceSelector,
        clusterOwner: String,
        job: V2FlinkClusterJobSpec
    ): V1FlinkJob {
        if (job.name == null) {
            throw RuntimeException("name is required")
        }

        if (job.spec == null) {
            throw RuntimeException("spec is required")
        }

        if (job.spec.bootstrap == null) {
            throw RuntimeException("bootstrap is required")
        }

        if (job.spec.savepoint == null) {
            throw RuntimeException("savepoint is required")
        }

        val jobLabels = mapOf(
            Pair("owner", clusterOwner),
            Pair("clusterName", clusterSelector.name),
            Pair("clusterUid", clusterSelector.uid),
            Pair("jobName", job.name),
            Pair("component", "flink")
        )

        val metadata = V1ObjectMetaBuilder()
            .withLabels(jobLabels)
            .withNamespace(clusterSelector.namespace)
            .withName("${clusterSelector.name}-${job.name}")
            .build()

        val resource = V1FlinkJob()
        resource.kind = "FlinkJob"
        resource.apiVersion = "nextbreakpoint.com/v1"
        resource.metadata = metadata
        resource.spec = job.spec

        return resource
    }
}