package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.JobSubmitConfig

class SubmitJob {
    fun run(apiConfig: ApiConfig, submitConfig: JobSubmitConfig) {
        try {
            val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
            val response = client.post("/submitJob")
                .putHeader("content-type", "application/json")
                .rxSendJson(submitConfig)
                .toBlocking()
                .value()
                .bodyAsString()
            println(response)
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}

