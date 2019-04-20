package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.TaskManagerDescriptor

class PostTaskManagerDetailsRequest {
    fun run(apiConfig: ApiConfig, taskManagerDescriptor: TaskManagerDescriptor) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
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

