package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.JobCancelConfig

class PostJobCancelRequest {
    fun run(apiConfig: ApiConfig, cancelConfig: JobCancelConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            val response = client.post("/job/cancel")
                .putHeader("content-type", "application/json")
                .rxSendJson(cancelConfig)
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

