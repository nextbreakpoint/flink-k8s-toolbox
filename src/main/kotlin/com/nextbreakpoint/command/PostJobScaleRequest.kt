package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.JobScaleParams

class PostJobScaleRequest {
    fun run(apiParams: ApiParams, scaleParams: JobScaleParams) {
        val client = createWebClient(host = apiParams.host, port = apiParams.port)
        try {
            val response = client.post("/job/scale")
                .putHeader("content-type", "application/json")
                .rxSendJson(scaleParams)
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

