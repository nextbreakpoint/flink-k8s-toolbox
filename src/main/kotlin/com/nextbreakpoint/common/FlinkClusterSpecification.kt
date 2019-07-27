package com.nextbreakpoint.common

import com.google.gson.GsonBuilder
import com.nextbreakpoint.model.DateTimeSerializer
import com.nextbreakpoint.model.V1FlinkClusterSpec
import org.joda.time.DateTime

object FlinkClusterSpecification {
    private val gson = GsonBuilder().registerTypeAdapter(DateTime::class.java, DateTimeSerializer()).create()

    fun parse(body: String): V1FlinkClusterSpec = gson.fromJson(body, V1FlinkClusterSpec::class.java)
}