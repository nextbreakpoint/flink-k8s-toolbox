package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.TaskManagerDescriptor

class PostTaskManagerDetailsRequest {
    fun run(apiParams: ApiParams, taskManagerDescriptor: TaskManagerDescriptor) {
        val client = createWebClient(host = apiParams.host, port = apiParams.port)
        try {
            val response = client.post("/taskmanager/details")
                .putHeader("content-type", "application/json")
                .rxSendJson(taskManagerDescriptor)
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

