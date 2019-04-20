package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.JobCancelParams

class PostJobCancelRequest {
    fun run(apiParams: ApiParams, cancelParams: JobCancelParams) {
        val client = createWebClient(host = apiParams.host, port = apiParams.port)
        try {
            val response = client.post("/job/cancel")
                .putHeader("content-type", "application/json")
                .rxSendJson(cancelParams)
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

