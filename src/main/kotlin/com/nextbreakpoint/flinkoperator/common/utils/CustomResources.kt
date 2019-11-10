package com.nextbreakpoint.flinkoperator.common.utils

import com.google.gson.GsonBuilder
import com.nextbreakpoint.flinkoperator.common.crd.DateTimeSerializer
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterList
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1RuntimeSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1BootstrapSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1JobManagerSpec
import com.nextbreakpoint.flinkoperator.common.crd.V1TaskManagerSpec
import org.joda.time.DateTime
import java.security.MessageDigest
import java.util.Base64

object CustomResources {
    private val gson = GsonBuilder().registerTypeAdapter(DateTime::class.java, DateTimeSerializer()).create()

    fun parseV1FlinkCluster(body: String): V1FlinkCluster = gson.fromJson(body, V1FlinkCluster::class.java)

    fun parseV1FlinkClusterList(body: String): V1FlinkClusterList = gson.fromJson(body, V1FlinkClusterList::class.java)

    fun parseV1FlinkClusterSpec(body: String): V1FlinkClusterSpec = gson.fromJson(body, V1FlinkClusterSpec::class.java)

    fun computeDigest(spec: V1RuntimeSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(
                    gson.toJson(spec).toByteArray())))
    }

    fun computeDigest(spec: V1BootstrapSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(
                    gson.toJson(spec).toByteArray())))
    }

    fun computeDigest(spec: V1JobManagerSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(
                    gson.toJson(spec).toByteArray())))
    }

    fun computeDigest(spec: V1TaskManagerSpec?): String {
        return if (spec == null) "" else String(
            Base64.getEncoder().encode(
                MessageDigest.getInstance("MD5").digest(
                    gson.toJson(spec).toByteArray())))
    }

    fun convertToMap(flinkCluster: V1FlinkCluster): Map<String, Any?> = gson.fromJson(gson.toJson(flinkCluster), Map::class.java) as Map<String, Any?>
}