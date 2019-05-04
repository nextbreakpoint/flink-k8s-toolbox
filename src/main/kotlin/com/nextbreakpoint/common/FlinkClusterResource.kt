package com.nextbreakpoint.common

import com.google.gson.GsonBuilder
import com.nextbreakpoint.model.DateTimeSerializer
import com.nextbreakpoint.model.V1FlinkCluster
import org.joda.time.DateTime

object FlinkClusterResource {
    private val gson = GsonBuilder().registerTypeAdapter(DateTime::class.java, DateTimeSerializer()).create()

    fun parse(body: String): V1FlinkCluster = gson.fromJson(body, V1FlinkCluster::class.java)
}