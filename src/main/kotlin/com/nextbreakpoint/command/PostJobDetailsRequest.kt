package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.JobDescriptor

class PostJobDetailsRequest {
    fun run(apiConfig: ApiConfig, jobDescriptor: JobDescriptor) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            val response = client.post("/job/details")
                .putHeader("content-type", "application/json")
                .rxSendJson(jobDescriptor)
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

