package com.nextbreakpoint

import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.models.V1DeleteOptions
import io.kubernetes.client.util.Config

fun main(args: Array<String>) {
    val client = Config.defaultClient()

    Configuration.setDefaultApiClient(client)

    val api = CoreV1Api()

    printAllPods(api)

    deletePod(api, args[0])
}

private fun printAllPods(api: CoreV1Api) {
    val list = api.listPodForAllNamespaces(null, null, null, null, null, null, null, 10, null)

    list.items.forEach {
        item -> println("Pod name: ${item.metadata.name}")
    }
}

private fun deletePod(api: CoreV1Api, name: String) {
    val status = api.deleteNamespacedPod(name, "default", V1DeleteOptions(), null, null, null, null)

    println("Response status: ${status.status}")
}

