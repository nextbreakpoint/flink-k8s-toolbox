package com.nextbreakpoint

import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.models.*
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream

fun main(args: Array<String>) {
    val client = Config.fromConfig(FileInputStream(File("/Users/andrea/Desktop/kubernetes-playground/admin.conf")))

    Configuration.setDefaultApiClient(client)

    val api = CoreV1Api()

    var pod = createPod(api)

//    printAllPods(api)

    deletePod(api, pod.metadata.name)
}

fun createPod(api: CoreV1Api) : V1Pod {
    val container = V1Container()
    container.image = "busybox"
    container.name = "mybox"
    container.command = listOf("ls", "/")
//    container.tty(true)
    val v1Pod = V1Pod()
        .metadata(V1ObjectMeta().generateName("busybox-"))
        .spec(V1PodSpec().containers(listOf(container)).restartPolicy("Never"))
    var pod = api.createNamespacedPod("default", v1Pod, null, null, null)
    println("Pod startus ${pod.metadata.name}")
    return pod;
}

private fun printAllPods(api: CoreV1Api) {
    val list = api.listPodForAllNamespaces(null, null, null, null, null, null, null, 10, null)

    list.items.forEach {
        item -> println("Pod name: ${item.metadata.name}")
    }

    list.items.forEach {
            item -> println("${item.status}")
    }
}

private fun deletePod(api: CoreV1Api, name: String) {
    val status = api.deleteNamespacedPod(name, "default", V1DeleteOptions(), "true", null, null, null, null)

    println("Response status: ${status.reason}")
    status.details.causes.forEach { println(it.message) }
}

