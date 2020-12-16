package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.k8s.crd.V1BootstrapSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeploymentSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec
import com.nextbreakpoint.flink.k8s.crd.V1JobManagerSpec
import com.nextbreakpoint.flink.k8s.crd.V1RestartSpec
import com.nextbreakpoint.flink.k8s.crd.V1RuntimeSpec
import com.nextbreakpoint.flink.k8s.crd.V1SavepointSpec
import com.nextbreakpoint.flink.k8s.crd.V1SupervisorSpec
import com.nextbreakpoint.flink.k8s.crd.V1TaskManagerSpec
import io.kubernetes.client.openapi.JSON
import java.security.MessageDigest
import java.util.Base64

object Resource {
    val RESOURCE_OWNER = "flink-operator"
    val OPERATOR_FINALIZER_VALUE = "operator.nextbreakpoint.com"
    val SUPERVISOR_FINALIZER_VALUE = "supervisor.nextbreakpoint.com"

    fun parseV1FlinkDeployment(body: String): V1FlinkDeployment = JSON().deserialize(body, V1FlinkDeployment::class.java)

    fun parseV1FlinkDeploymentSpec(body: String): V1FlinkDeploymentSpec = JSON().deserialize(body, V1FlinkDeploymentSpec::class.java)

    fun parseV1FlinkCluster(body: String): V1FlinkCluster = JSON().deserialize(body, V1FlinkCluster::class.java)

    fun parseV1FlinkClusterSpec(body: String): V1FlinkClusterSpec = JSON().deserialize(body, V1FlinkClusterSpec::class.java)

    fun parseV1FlinkJob(body: String): V1FlinkJob = JSON().deserialize(body, V1FlinkJob::class.java)

    fun parseV1FlinkJobSpec(body: String): V1FlinkJobSpec = JSON().deserialize(body, V1FlinkJobSpec::class.java)

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

    fun computeDigest(spec: V1SavepointSpec?): String {
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

    fun computeDigest(spec: V1SupervisorSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(JSON().serialize(spec).toByteArray())
            )
        )
    }

    fun computeDigest(spec: V1RestartSpec?): String {
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

    fun computeDigest(spec: V1FlinkDeploymentSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(JSON().serialize(spec).toByteArray())
            )
        )
    }

    fun computeDigest(spec: V1FlinkClusterSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(JSON().serialize(spec).toByteArray())
            )
        )
    }

    fun makeLabelSelector(clusterName: String) =
        "clusterName=$clusterName,owner=flink-operator,component=flink"

    fun makeLabelSelector(clusterName: String, jobName: String) =
        "clusterName=$clusterName,jobName=$jobName,owner=flink-operator,component=flink"
}