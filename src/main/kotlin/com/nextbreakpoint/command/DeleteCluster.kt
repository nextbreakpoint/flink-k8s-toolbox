package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.CommandUtils.forwardPort
import com.nextbreakpoint.model.ClusterDescriptor
import io.kubernetes.client.apis.CoreV1Api
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel

class DeleteCluster {
    @ExperimentalCoroutinesApi
    fun run(namespace: String, descriptor: ClusterDescriptor) {
        try {
            val coreApi = CoreV1Api()

            val pods = coreApi.listNamespacedPod(
                namespace,
                null,
                null,
                null,
                null,
                "app=flink-submit",
                1,
                null,
                30,
                null
            )

            if (!pods.items.isEmpty()) {
                val pod = pods.items.get(0)

                println("Found pod ${pod.metadata.name}")

                val stop = Channel<Int>()
                val localPort = 34444
                val process = forwardPort(pod, localPort, 4444, stop)
                process.start()
                // We need to wait for the server socket to be ready
                Thread.sleep(1000)
                val client = createWebClient(port = localPort)
                val response = client.post("/deleteCluster")
                    .putHeader("content-type", "application/json")
                    .rxSendJson(descriptor)
                    .toBlocking()
                    .value()
                    .bodyAsString()
                println(response)
                // Close server socket
                stop.close()
                process.join()
                client.close()
            } else {
                println("FlinkSubmit pod not found!")
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}

