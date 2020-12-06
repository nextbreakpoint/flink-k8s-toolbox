package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder

object DeploymentResourcesDefaultFactory : DeploymentResourcesFactory {
    override fun createFlinkCluster(
        namespace: String,
        owner: String,
        clusterName: String,
        clusterSpec: V1FlinkClusterSpec
    ): V1FlinkCluster {
        if (clusterSpec.runtime == null) {
            throw RuntimeException("runtime is required")
        }

        if (clusterSpec.jobManager == null) {
            throw RuntimeException("jobManager is required")
        }

        if (clusterSpec.taskManager == null) {
            throw RuntimeException("taskManager is required")
        }

        if (clusterSpec.supervisor == null) {
            throw RuntimeException("supervisor is required")
        }

        val jobLabels = mapOf(
            Pair("owner", owner),
            Pair("clusterName", clusterName),
            Pair("component", "flink")
        )

        val metadata = V1ObjectMetaBuilder()
            .withLabels(jobLabels)
            .withNamespace(namespace)
            .withName(clusterName)
            .build()

        return V1FlinkCluster.builder()
            .withKind("FlinkCluster")
            .withApiVersion("nextbreakpoint.com/v1")
            .withMetadata(metadata)
            .withSpec(clusterSpec)
            .build()
    }

    override fun createFlinkJob(
        namespace: String,
        owner: String,
        clusterName: String,
        jobName: String,
        jobSpec: V1FlinkJobSpec
    ): V1FlinkJob {
        if (jobSpec.bootstrap == null) {
            throw RuntimeException("bootstrap is required")
        }

        if (jobSpec.savepoint == null) {
            throw RuntimeException("savepoint is required")
        }

        val jobLabels = mapOf(
            Pair("owner", owner),
            Pair("clusterName", clusterName),
            Pair("jobName", jobName),
            Pair("component", "flink")
        )

        val metadata = V1ObjectMetaBuilder()
            .withLabels(jobLabels)
            .withNamespace(namespace)
            .withName("$clusterName-$jobName")
            .build()

        return V1FlinkJob.builder()
            .withKind("FlinkJob")
            .withApiVersion("nextbreakpoint.com/v1")
            .withMetadata(metadata)
            .withSpec(jobSpec)
            .build()
    }
}