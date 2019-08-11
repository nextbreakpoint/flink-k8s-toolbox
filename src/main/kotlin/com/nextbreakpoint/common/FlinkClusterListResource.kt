package com.nextbreakpoint.common

import com.google.gson.GsonBuilder
import com.nextbreakpoint.model.DateTimeSerializer
import com.nextbreakpoint.model.V1FlinkClusterList
import org.joda.time.DateTime

object FlinkClusterListResource {
    private val gson = GsonBuilder().registerTypeAdapter(DateTime::class.java, DateTimeSerializer()).create()

    fun parse(body: String): V1FlinkClusterList = gson.fromJson(body, V1FlinkClusterList::class.java)
}