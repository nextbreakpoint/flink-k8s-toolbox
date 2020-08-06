package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.k8s.crd.V1BootstrapSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.crd.V1JobManagerSpec
import com.nextbreakpoint.flink.k8s.crd.V1OperatorSpec
import com.nextbreakpoint.flink.k8s.crd.V1RuntimeSpec
import com.nextbreakpoint.flink.k8s.crd.V1TaskManagerSpec
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec
import com.nextbreakpoint.flink.k8s.crd.V1SupervisorSpec
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterSpec
import io.kubernetes.client.openapi.JSON
import java.security.MessageDigest
import java.util.Base64

object Resource {
    fun parseV1FlinkCluster(body: String): V1FlinkCluster = JSON().deserialize(body, V1FlinkCluster::class.java)

    fun parseV2FlinkCluster(body: String): V2FlinkCluster = JSON().deserialize(body, V2FlinkCluster::class.java)

    fun parseV1FlinkClusterSpec(body: String): V1FlinkClusterSpec = JSON().deserialize(body, V1FlinkClusterSpec::class.java)

    fun parseV2FlinkClusterSpec(body: String): V2FlinkClusterSpec = JSON().deserialize(body, V2FlinkClusterSpec::class.java)

    fun parseV1FlinkJob(body: String): V1FlinkJob = JSON().deserialize(body, V1FlinkJob::class.java)

    fun computeDigest(spec: V1RuntimeSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(JSON().serialize(spec).toByteArray())
            )
        )
    }

    fun computeDigest(spec: V1BootstrapSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(JSON().serialize(spec).toByteArray())
            )
        )
    }

    fun computeDigest(spec: V1JobManagerSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(JSON().serialize(spec).toByteArray())
            )
        )
    }

    fun computeDigest(spec: V1TaskManagerSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(JSON().serialize(spec).toByteArray())
            )
        )
    }

    fun computeDigest(spec: V1OperatorSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(JSON().serialize(spec).toByteArray())
            )
        )
    }

    fun computeDigest(spec: V1SupervisorSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(JSON().serialize(spec).toByteArray())
            )
        )
    }

    fun computeDigest(spec: V1FlinkJobSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(JSON().serialize(spec).toByteArray())
            )
        )
    }

    fun makeLabelSelector(clusterSelector: ResourceSelector) =
        "uid=${clusterSelector.uid},name=${clusterSelector.name},owner=flink-operator,component=flink,role=taskmanager"
}