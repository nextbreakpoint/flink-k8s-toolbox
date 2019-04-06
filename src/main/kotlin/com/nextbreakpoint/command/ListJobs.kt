package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.JobListConfig

class ListJobs {
    fun run(apiConfig: ApiConfig, listConfig: JobListConfig) {
        try {
            val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
            val response = client.post("/listJobs")
                .putHeader("content-type", "application/json")
                .rxSendJson(listConfig)
                .toBlocking()
                .value()
                .bodyAsString()
            println(response)
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}

