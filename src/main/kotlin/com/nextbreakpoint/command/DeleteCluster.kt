package com.nextbreakpoint.command

import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.models.*
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream

class DeleteCluster {
    private val NAMESPACE = "default"

    fun run(kubeConfigPath: String, clusterName: String) {
        val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))

        Configuration.setDefaultApiClient(client)

        deleteCluster(clusterName)
    }

    private fun deleteCluster(clusterName: String) {
        println("Deleting cluster $clusterName...")

        val api = AppsV1Api()

        val statefulSets = api.listNamespacedStatefulSet(
            NAMESPACE,
            null,
            null,
            null,
            null,
            "cluster=$clusterName",
            null,
            null,
            30,
            null
        )

        statefulSets.items.forEach { statefulSet ->
            try {
                println("Removing StatefulSet ${statefulSet.metadata.name}...")

                val status = api.deleteNamespacedStatefulSet(
                    statefulSet.metadata.name,
                    NAMESPACE,
                    V1DeleteOptions(),
                    "true",
                    null,
                    null,
                    null,
                    null
                )

                println("Response status: ${status.reason}")

                status.details.causes.forEach { println(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }

        val coreApi = CoreV1Api()

        val services = coreApi.listNamespacedService(
            NAMESPACE,
            null,
            null,
            null,
            null,
            "cluster=$clusterName",
            null,
            null,
            30,
            null
        )

        services.items.forEach { service ->
            try {
                println("Removing Service ${service.metadata.name}...")

                val status = coreApi.deleteNamespacedService(
                    service.metadata.name,
                    NAMESPACE,
                    V1DeleteOptions(),
                    "true",
                    null,
                    null,
                    null,
                    null
                )

                println("Response status: ${status.reason}")

                status.details.causes.forEach { println(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }

        val volumeClaims = coreApi.listNamespacedPersistentVolumeClaim(
            NAMESPACE,
            null,
            null,
            null,
            null,
            "cluster=$clusterName",
            null,
            null,
            30,
            null
        )

        volumeClaims.items.forEach { volumeClaim ->
            try {
                println("Removing Persistent Volume Claim ${volumeClaim.metadata.name}...")

                val status = coreApi.deleteNamespacedPersistentVolumeClaim(
                    volumeClaim.metadata.name,
                    NAMESPACE,
                    V1DeleteOptions(),
                    "true",
                    null,
                    null,
                    null,
                    null
                )

                println("Response status: ${status.reason}")

                status.details.causes.forEach { println(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }

        println("Done.")
    }
}

