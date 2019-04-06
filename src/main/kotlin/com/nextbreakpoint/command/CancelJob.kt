package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.JobCancelConfig

class CancelJob {
    fun run(apiConfig: ApiConfig, cancelConfig: JobCancelConfig) {
        try {
            val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
            val response = client.post("/cancelJob")
                .putHeader("content-type", "application/json")
                .rxSendJson(cancelConfig)
                .toBlocking()
                .value()
                .bodyAsString()
            println(response)
            // Close server socket
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}

