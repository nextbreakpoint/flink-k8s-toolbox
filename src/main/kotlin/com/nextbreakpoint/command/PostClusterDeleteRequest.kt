package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.ClusterDescriptor

class PostClusterDeleteRequest {
    fun run(apiParams: ApiParams, descriptor: ClusterDescriptor) {
        val client = createWebClient(host = apiParams.host, port = apiParams.port)
        try {
            val response = client.post("/cluster/delete")
                .putHeader("content-type", "application/json")
                .rxSendJson(descriptor)
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

