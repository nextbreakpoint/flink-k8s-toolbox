package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiConfig
import com.nextbreakpoint.model.ClusterConfig

class PostClusterCreateRequest {
    fun run(apiConfig: ApiConfig, clusterConfig: ClusterConfig) {
        val client = createWebClient(host = apiConfig.host, port = apiConfig.port)
        try {
            val response = client.post("/cluster/create")
                .putHeader("content-type", "application/json")
                .rxSendJson(clusterConfig)
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

