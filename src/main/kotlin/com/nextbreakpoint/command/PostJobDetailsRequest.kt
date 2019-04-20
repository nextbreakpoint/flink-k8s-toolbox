package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.JobDescriptor

class PostJobDetailsRequest {
    fun run(apiParams: ApiParams, jobDescriptor: JobDescriptor) {
        val client = createWebClient(host = apiParams.host, port = apiParams.port)
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

