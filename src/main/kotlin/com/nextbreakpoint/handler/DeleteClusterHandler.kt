package com.nextbreakpoint.handler

import com.nextbreakpoint.model.ClusterDescriptor
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.models.V1DeleteOptions

object DeleteClusterHandler {
    fun execute(descriptor: ClusterDescriptor): String {
        try {
            val api = AppsV1Api()

            val coreApi = CoreV1Api()

            println("Deleting cluster ${descriptor.name}...")

            deleteStatefulSets(api, descriptor)

            deleteService(coreApi, descriptor)

            deletePersistentVolumeClaims(coreApi, descriptor)

            println("Done.")

            return "{\"status\":\"SUCCESS\"}"
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    private fun deleteStatefulSets(api: AppsV1Api, descriptor: ClusterDescriptor) {
        val statefulSets = api.listNamespacedStatefulSet(
            descriptor.namespace,
            null,
            null,
            null,
            null,
            "cluster=${descriptor.name},environment=${descriptor.environment}",
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
                    descriptor.namespace,
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
    }

    private fun deleteService(coreApi: CoreV1Api, descriptor: ClusterDescriptor) {
        val services = coreApi.listNamespacedService(
            descriptor.namespace,
            null,
            null,
            null,
            null,
            "cluster=${descriptor.name},environment=${descriptor.environment}",
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
                    descriptor.namespace,
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
    }

    private fun deletePersistentVolumeClaims(coreApi: CoreV1Api, descriptor: ClusterDescriptor) {
        val volumeClaims = coreApi.listNamespacedPersistentVolumeClaim(
            descriptor.namespace,
            null,
            null,
            null,
            null,
            "cluster=${descriptor.name},environment=${descriptor.environment}",
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
                    descriptor.namespace,
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
    }
}