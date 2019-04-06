package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.ClusterDescriptor

class DeleteCluster {
    fun run(apiConfig: ApiConfig, descriptor: ClusterDescriptor) {
        try {
            val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
            val response = client.post("/deleteCluster")
                .putHeader("content-type", "application/json")
                .rxSendJson(descriptor)
                .toBlocking()
                .value()
                .bodyAsString()
            println(response)
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}

