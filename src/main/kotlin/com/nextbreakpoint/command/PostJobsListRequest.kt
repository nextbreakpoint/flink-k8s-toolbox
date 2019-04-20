package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.JobListConfig

class PostJobsListRequest {
    fun run(apiConfig: ApiConfig, listConfig: JobListConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            val response = client.post("/jobs/list")
                .putHeader("content-type", "application/json")
                .rxSendJson(listConfig)
                .toBlocking()
                .value()
            println(response.bodyAsString())
        } catch (e: Exception) {
            throw RuntimeException(e)
        } finally {
            client.close()
        }
    }
}

