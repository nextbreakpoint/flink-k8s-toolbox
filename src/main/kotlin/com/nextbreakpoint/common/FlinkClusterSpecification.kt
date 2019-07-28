package com.nextbreakpoint.common

import com.google.gson.GsonBuilder
import com.nextbreakpoint.model.DateTimeSerializer
import com.nextbreakpoint.model.V1FlinkClusterSpec
import com.nextbreakpoint.model.V1FlinkImageSpec
import com.nextbreakpoint.model.V1FlinkJobSpec
import com.nextbreakpoint.model.V1JobManagerSpec
import com.nextbreakpoint.model.V1TaskManagerSpec
import org.joda.time.DateTime
import java.security.MessageDigest
import java.util.Base64

object FlinkClusterSpecification {
    private val gson = GsonBuilder().registerTypeAdapter(DateTime::class.java, DateTimeSerializer()).create()

    fun parse(body: String): V1FlinkClusterSpec = gson.fromJson(body, V1FlinkClusterSpec::class.java)

    fun computeDigest(spec: V1FlinkClusterSpec?): String {
        return if (spec == null) "" else String(Base64.getEncoder().encode(MessageDigest.getInstance("MD5").digest(gson.toJson(spec).toByteArray())))
    }

    fun computeDigest(spec: V1FlinkImageSpec?): String {
        return if (spec == null) "" else String(Base64.getEncoder().encode(MessageDigest.getInstance("MD5").digest(gson.toJson(spec).toByteArray())))
    }

    fun computeDigest(spec: V1FlinkJobSpec?): String {
        return if (spec == null) "" else String(Base64.getEncoder().encode(MessageDigest.getInstance("MD5").digest(gson.toJson(spec).toByteArray())))
    }

    fun computeDigest(spec: V1JobManagerSpec?): String {
        return if (spec == null) "" else String(Base64.getEncoder().encode(MessageDigest.getInstance("MD5").digest(gson.toJson(spec).toByteArray())))
    }

    fun computeDigest(spec: V1TaskManagerSpec?): String {
        return if (spec == null) "" else String(Base64.getEncoder().encode(MessageDigest.getInstance("MD5").digest(gson.toJson(spec).toByteArray())))
    }
}