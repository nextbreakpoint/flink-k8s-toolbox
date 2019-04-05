package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.CommandUtils.forwardPort
import com.nextbreakpoint.model.JobListConfig
import io.kubernetes.client.apis.CoreV1Api
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel

class ListJobs {
    @ExperimentalCoroutinesApi
    fun run(listConfig: JobListConfig) {
        try {
            val coreApi = CoreV1Api()

            val services = coreApi.listNamespacedService(
                null,
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

            if (!services.items.isEmpty()) {
                val service = services.items.get(0)

                println("Found service ${service.metadata.name}")

                val pods = coreApi.listNamespacedPod(
                    null,
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
                    val client = CommandUtils.createWebClient(localPort)
                    client.post("/listJobs")
                        .putHeader("content-type", "application/json")
                        .rxSendJson(listConfig).subscribe({
                            println(Gson().toJson(it.bodyAsString()))
                        }, {
                            println(it.message)
                        })
                    client.close()
                    // Close server socket
                    stop.close()
                    process.join()
                }
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}

