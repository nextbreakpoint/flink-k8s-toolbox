package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.RunJobConfig

class PostJobRunRequest {
    fun run(apiConfig: ApiConfig, runJobConfig: RunJobConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            val response = client.post("/job/run")
                .putHeader("content-type", "application/json")
                .rxSendJson(runJobConfig)
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

