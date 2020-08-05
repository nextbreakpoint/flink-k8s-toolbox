package com.nextbreakpoint.flinkoperator.server.common

import com.nextbreakpoint.flinkoperator.common.crd.V1BootstrapSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterList
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1JobManagerSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1OperatorSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1RuntimeSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1TaskManagerSpec
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import io.kubernetes.client.JSON
import java.security.MessageDigest
import java.util.Base64

object Resource {
    fun parseV1FlinkCluster(body: String): V1FlinkCluster = JSON().deserialize(body, V1FlinkCluster::class.java)

    fun parseV1FlinkClusterList(body: String): V1FlinkClusterList = JSON().deserialize(body, V1FlinkClusterList::class.java)

    fun parseV1FlinkClusterSpec(body: String): V1FlinkClusterSpec = JSON().deserialize(body, V1FlinkClusterSpec::class.java)

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

    fun makeLabelSelector(clusterSelector: ClusterSelector) =
        "uid=${clusterSelector.uuid},name=${clusterSelector.name},owner=flink-operator,component=flink,role=taskmanager"
}