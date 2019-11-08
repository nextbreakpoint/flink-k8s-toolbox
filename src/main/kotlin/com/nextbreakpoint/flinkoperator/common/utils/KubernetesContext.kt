package com.nextbreakpoint.flinkoperator.common.utils

import com.google.common.io.ByteStreams
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import io.kubernetes.client.ApiClient
import io.kubernetes.client.ApiException
import io.kubernetes.client.ApiResponse
import io.kubernetes.client.Configuration
import io.kubernetes.client.PortForward
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.apis.BatchV1Api
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.apis.CustomObjectsApi
import io.kubernetes.client.custom.V1Patch
import io.kubernetes.client.models.V1DeleteOptions
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1JobList
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Pod
import io.kubernetes.client.models.V1PodList
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1ServiceList
import io.kubernetes.client.models.V1StatefulSet
import io.kubernetes.client.models.V1StatefulSetList
import io.kubernetes.client.util.Config
import io.kubernetes.client.util.Watch
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import org.apache.log4j.Logger
import java.io.File
import java.io.FileInputStream
import java.net.ServerSocket
import java.util.concurrent.TimeUnit

object KubernetesContext {
    private val logger = Logger.getLogger(KubernetesContext::class.simpleName)

    private val objectApi = CustomObjectsApi()
    private val batchApi = BatchV1Api()
    private val coreApi = CoreV1Api()
    private val appsApi = AppsV1Api()

    private val objectApiWatch = CustomObjectsApi()
    private val batchApiWatch = BatchV1Api()
    private val coreApiWatch = CoreV1Api()
    private val appsApiWatch = AppsV1Api()

    fun configure(kubeConfig: String?) {
        Configuration.setDefaultApiClient(createKubernetesClient(kubeConfig, 10000))
        objectApi.apiClient = Configuration.getDefaultApiClient()
        batchApi.apiClient = Configuration.getDefaultApiClient()
        coreApi.apiClient = Configuration.getDefaultApiClient()
        appsApi.apiClient = Configuration.getDefaultApiClient()
        objectApiWatch.apiClient = createKubernetesClient(kubeConfig, 0)
        batchApiWatch.apiClient = createKubernetesClient(kubeConfig, 0)
        coreApiWatch.apiClient = createKubernetesClient(kubeConfig, 0)
        appsApiWatch.apiClient = createKubernetesClient(kubeConfig, 0)
    }

    fun findFlinkAddress(flinkOptions: FlinkOptions, namespace: String, clusterName: String): FlinkAddress {
        var jobmanagerHost = flinkOptions.hostname ?: "localhost"
        var jobmanagerPort = flinkOptions.portForward ?: 8081

        if (flinkOptions.hostname == null && flinkOptions.portForward == null && flinkOptions.useNodePort) {
            val nodes = coreApi.listNode(
                null,
                null,
                null,
                null,
                1,
                null,
                30,
                null
            )

            if (!nodes.items.isEmpty()) {
                nodes.items.get(0).status.addresses.filter {
                    it.type.equals("InternalIP")
                }.map {
                    it.address
                }.firstOrNull()?.let {
                    jobmanagerHost = it
                }
            } else {
                throw RuntimeException("Node not found")
            }
        }

        if (flinkOptions.portForward == null) {
            val call = objectApi.getNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
                clusterName,
                null,
                null
            )

            val response = call.execute()

            if (!response.isSuccessful) {
                response.body().use { it.source().use { source -> logger.error(source.readUtf8Line()) } }
                throw RuntimeException("Can't fetch custom object $clusterName")
            }

            response.body().use { it.source().use { source ->
                val flinkCluster = CustomResources.parseV1FlinkCluster(source.readUtf8Line())

                val clusterId = flinkCluster.metadata.uid

                val services = coreApi.listNamespacedService(
                    namespace,
                    null,
                    null,
                    null,
                    "name=$clusterName,uid=$clusterId,role=jobmanager",
                    1,
                    null,
                    30,
                    null
                )

                if (!services.items.isEmpty()) {
                    val service = services.items.get(0)

                    logger.debug("Found JobManager service ${service.metadata.name}")

                    if (flinkOptions.useNodePort) {
                        service.spec.ports.filter {
                            it.name.equals("ui")
                        }.filter {
                            it.nodePort != null
                        }.map {
                            it.nodePort
                        }.firstOrNull()?.let {
                            jobmanagerPort = it
                        }
                    } else {
                        service.spec.ports.filter {
                            it.name.equals("ui")
                        }.filter {
                            it.port != null
                        }.map {
                            it.port
                        }.firstOrNull()?.let {
                            jobmanagerPort = it
                        }
                        jobmanagerHost = service.spec.clusterIP
                    }
                } else {
                    throw RuntimeException("JobManager service not found (name=$clusterName, id=$clusterId)")
                }

                val pods = coreApi.listNamespacedPod(
                    namespace,
                    null,
                    null,
                    null,
                    "name=$clusterName,uid=$clusterId,role=jobmanager",
                    1,
                    null,
                    30,
                    null
                )

                if (!pods.items.isEmpty()) {
                    val pod = pods.items.get(0)

                    logger.debug("Found JobManager pod ${pod.metadata.name}")
                } else {
                    throw RuntimeException("JobManager pod not found (name=$clusterName, id=$clusterId)")
                }
            }
        } }

        logger.debug("Flink client created for host $jobmanagerHost and port $jobmanagerPort")

        return FlinkAddress(
            host = jobmanagerHost,
            port = jobmanagerPort
        )
    }

    fun updateAnnotations(clusterId: ClusterId, annotations: Map<String, String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "annotations" to annotations
            )
        )

        val response = objectApi.patchNamespacedCustomObjectCall(
            "nextbreakpoint.com",
            "v1",
            clusterId.namespace,
            "flinkclusters",
            clusterId.name,
            patch,
            null,
            null
        ).execute()

        if (!response.isSuccessful) {
            response.body().use { it.source().use { source -> logger.error(source.readUtf8Line()) } }
            throw RuntimeException("Can't update annotations of cluster ${clusterId.name}")
        }
    }

    fun updateStatus(clusterId: ClusterId, status: V1FlinkClusterStatus) {
        val patch = mapOf<String, Any?>(
            "status" to status
        )

        val response = objectApi.patchNamespacedCustomObjectStatusCall(
            "nextbreakpoint.com",
            "v1",
            clusterId.namespace,
            "flinkclusters",
            clusterId.name,
            patch,
            null,
            null
        ).execute()

        if (!response.isSuccessful) {
            response.body().use { it.source().use { source -> logger.error(source.readUtf8Line()) } }
            throw RuntimeException("Can't update status of cluster ${clusterId.name}")
        }
    }

    fun watchFlickClusters(namespace: String): Watch<V1FlinkCluster> =
        Watch.createWatch(
            objectApiWatch.apiClient,
            objectApiWatch.listNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
                null,
                null,
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1FlinkCluster>>() {}.type
        )

    fun watchServices(namespace: String): Watch<V1Service> =
        Watch.createWatch(
            coreApiWatch.apiClient,
            coreApiWatch.listNamespacedServiceCall(
                namespace,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1Service>>() {}.type
        )

    fun watchDeployments(namespace: String): Watch<V1Deployment> =
        Watch.createWatch(
            appsApiWatch.apiClient,
            appsApiWatch.listNamespacedDeploymentCall(
                namespace,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1Deployment>>() {}.type
        )

    fun watchJobs(namespace: String): Watch<V1Job> =
        Watch.createWatch(
            batchApiWatch.apiClient,
            batchApiWatch.listNamespacedJobCall(
                namespace,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1Job>>() {}.type
        )

    fun watchStatefulSets(namespace: String): Watch<V1StatefulSet> =
        Watch.createWatch(
            appsApiWatch.apiClient,
            appsApiWatch.listNamespacedStatefulSetCall(
                namespace,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1StatefulSet>>() {}.type
        )

    fun watchPermanentVolumeClaims(namespace: String): Watch<V1PersistentVolumeClaim> =
        Watch.createWatch(
            coreApiWatch.apiClient,
            coreApiWatch.listNamespacedPersistentVolumeClaimCall(
                namespace,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1PersistentVolumeClaim>>() {}.type
        )

    fun listFlinkClusters(namespace: String): List<V1FlinkCluster> {
        val response = objectApi.listNamespacedCustomObjectCall(
            "nextbreakpoint.com",
            "v1",
            namespace,
            "flinkclusters",
            null,
            null,
            null,
            null,
            5,
            null,
            null,
            null
        ).execute()

        if (!response.isSuccessful) {
            response.body().use { it.source().use { source -> logger.error(source.readUtf8Line()) } }
            throw RuntimeException("Can't fetch custom objects")
        }

        return response.body().use { it.source().use { source ->
            CustomResources.parseV1FlinkClusterList(source.readUtf8Line()).items
        } }
    }

    fun getFlinkCluster(namespace: String, name: String): V1FlinkCluster {
        val response = objectApi.getNamespacedCustomObjectCall(
            "nextbreakpoint.com",
            "v1",
            namespace,
            "flinkclusters",
            name,
            null,
            null
        ).execute()

        if (!response.isSuccessful) {
            response.body().use { it.source().use { source -> logger.error(source.readUtf8Line()) } }
            throw RuntimeException("Can't fetch custom object $name")
        }

        return response.body().use { it.source().use { source ->
            CustomResources.parseV1FlinkCluster(source.readUtf8Line())
        } }
    }

    fun listJobResources(namespace: String): List<V1Job> {
        return batchApi.listNamespacedJob(
            namespace,
            null,
            null,
            null,
            "component=flink,owner=flink-operator",
            null,
            null,
            5,
            null
        ).items
    }

    fun listServiceResources(namespace: String): List<V1Service> {
        return coreApi.listNamespacedService(
            namespace,
            null,
            null,
            null,
            "component=flink,owner=flink-operator",
            null,
            null,
            5,
            null
        ).items
    }

    fun listDeploymentResources(namespace: String): List<V1Deployment> {
        return appsApi.listNamespacedDeployment(
            namespace,
            null,
            null,
            null,
            "component=flink,owner=flink-operator",
            null,
            null,
            5,
            null
        ).items
    }

    fun listStatefulSetResources(namespace: String): List<V1StatefulSet> {
        return appsApi.listNamespacedStatefulSet(
            namespace,
            null,
            null,
            null,
            "component=flink,owner=flink-operator",
            null,
            null,
            5,
            null
        ).items
    }

    fun listPermanentVolumeClaimResources(namespace: String): List<V1PersistentVolumeClaim> {
        return coreApi.listNamespacedPersistentVolumeClaim(
            namespace,
            null,
            null,
            null,
            "component=flink,owner=flink-operator",
            null,
            null,
            5,
            null
        ).items
    }

    fun listJobManagerServices(clusterId: ClusterId): V1ServiceList {
        return coreApi.listNamespacedService(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator",
            null,
            null,
            5,
            null
        )
    }

    fun listJobManagerStatefulSets(clusterId: ClusterId): V1StatefulSetList {
        return appsApi.listNamespacedStatefulSet(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,role=jobmanager",
            null,
            null,
            5,
            null
        )
    }

    fun listTaskManagerStatefulSets(clusterId: ClusterId): V1StatefulSetList {
        return appsApi.listNamespacedStatefulSet(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,role=taskmanager",
            null,
            null,
            5,
            null
        )
    }

    fun createJobManagerService(
        clusterId: ClusterId,
        resources: ClusterResources
    ): V1Service {
        return coreApi.createNamespacedService(
            clusterId.namespace,
            resources.jobmanagerService,
            null,
            null,
            null
        )
    }

    fun createJobManagerStatefulSet(
        clusterId: ClusterId,
        resources: ClusterResources
    ): V1StatefulSet {
        return appsApi.createNamespacedStatefulSet(
            clusterId.namespace,
            resources.jobmanagerStatefulSet,
            null,
            null,
            null
        )
    }

    fun createTaskManagerStatefulSet(
        clusterId: ClusterId,
        resources: ClusterResources
    ): V1StatefulSet {
        return appsApi.createNamespacedStatefulSet(
            clusterId.namespace,
            resources.taskmanagerStatefulSet,
            null,
            null,
            null
        )
    }

    fun deleteServices(clusterId: ClusterId) {
        val services = coreApi.listNamespacedService(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator",
            null,
            null,
            5,
            null
        )

        val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

        services.items.forEach { service ->
            try {
                logger.info("Removing Service ${service.metadata.name}...")

                val status = coreApi.deleteNamespacedService(
                    service.metadata.name,
                    clusterId.namespace,
                    null,
                    deleteOptions,
                    null,
                    5,
                    null,
                    null
                )

                logger.debug("Response status: ${status.reason}")

                status.details.causes.forEach { logger.info(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }
    }

    fun deleteStatefulSets(clusterId: ClusterId) {
        val statefulSets = appsApi.listNamespacedStatefulSet(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator",
            null,
            null,
            5,
            null
        )

        val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

        statefulSets.items.forEach { statefulSet ->
            try {
                logger.info("Removing StatefulSet ${statefulSet.metadata.name}...")

                val status = appsApi.deleteNamespacedStatefulSet(
                    statefulSet.metadata.name,
                    clusterId.namespace,
                    null,
                    deleteOptions,
                    null,
                    5,
                    null,
                    null
                )

                logger.debug("Response status: ${status.reason}")

                status.details.causes.forEach { logger.info(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }
    }

    fun deletePersistentVolumeClaims(clusterId: ClusterId) {
        val volumeClaims = coreApi.listNamespacedPersistentVolumeClaim(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator",
            null,
            null,
            5,
            null
        )

        val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

        volumeClaims.items.forEach { volumeClaim ->
            try {
                logger.info("Removing Persistent Volume Claim ${volumeClaim.metadata.name}...")

                val status = coreApi.deleteNamespacedPersistentVolumeClaim(
                    volumeClaim.metadata.name,
                    clusterId.namespace,
                    null,
                    deleteOptions,
                    null,
                    5,
                    null,
                    null
                )

                logger.debug("Response status: ${status.reason}")

                status.details.causes.forEach { logger.info(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }
    }

    fun deleteUploadJobs(clusterId: ClusterId) {
        val jobs = batchApi.listNamespacedJob(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator",
            null,
            null,
            5,
            null
        )

        val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

        jobs.items.forEach { job ->
            try {
                logger.info("Removing Job ${job.metadata.name}...")

                val status = batchApi.deleteNamespacedJob(
                    job.metadata.name,
                    clusterId.namespace,
                    null,
                    deleteOptions,
                    null,
                    5,
                    null,
                    null
                )

                logger.debug("Response status: ${status.reason}")

                status.details.causes.forEach { logger.info(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }
    }

    fun updateSavepointPath(clusterId: ClusterId, savepointPath: String) {
        val patch = mapOf<String, Any?>(
            "spec" to mapOf<String, Any?>(
                "flinkOperator" to mapOf<String, Any?>(
                    "savepointPath" to savepointPath
                )
            )
        )

        val response = objectApi.patchNamespacedCustomObjectCall(
            "nextbreakpoint.com",
            "v1",
            clusterId.namespace,
            "flinkclusters",
            clusterId.name,
            patch,
            null,
            null
        ).execute()

        if (response.isSuccessful) {
            logger.info("Savepoint of cluster ${clusterId.name} updated to $savepointPath")
        } else {
            response.body().use { it.source().use { source -> logger.error(source.readUtf8Line()) } }
            logger.error("Can't update savepoint of cluster ${clusterId.name}")
        }
    }

    fun createFlinkCluster(flinkCluster: V1FlinkCluster): ApiResponse<Any> {
        try {
            return objectApi.createNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                flinkCluster.metadata.namespace,
                "flinkclusters",
                CustomResources.convertToMap(flinkCluster) /* oh boy, it works with map but not with json or pojo !!! */,
                null
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun deleteFlinkCluster(clusterId: ClusterId): ApiResponse<Any> {
        try {
            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            return objectApi.deleteNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                clusterId.namespace,
                "flinkclusters",
                clusterId.name,
                deleteOptions,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun listUploadJobs(clusterId: ClusterId): V1JobList {
        return batchApi.listNamespacedJob(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,job-name=flink-upload-${clusterId.name}",
            null,
            null,
            5,
            null
        )
    }

    fun createUploadJob(clusterId: ClusterId, params: ClusterResources): V1Job {
        return batchApi.createNamespacedJob(
            clusterId.namespace,
            params.jarUploadJob,
            null,
            null,
            null
        )
    }

    fun listTaskManagerPods(clusterId: ClusterId): V1PodList {
        val taskmanagerPods = coreApi.listNamespacedPod(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,role=taskmanager",
            null,
            null,
            5,
            null
        )
        return taskmanagerPods
    }

    fun listJobManagerPods(clusterId: ClusterId): V1PodList {
        val jobmanagerPods = coreApi.listNamespacedPod(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,role=jobmanager",
            null,
            null,
            5,
            null
        )
        return jobmanagerPods
    }

    fun restartJobManagerStatefulSets(
        clusterId: ClusterId,
        resources: ClusterResources
    ) {
        val statefulSets = appsApi.listNamespacedStatefulSet(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,role=jobmanager",
            null,
            null,
            5,
            null
        )

        statefulSets.items.forEach { statefulSet ->
            try {
                logger.info("Scaling StatefulSet ${statefulSet.metadata.name}...")

                val patch = listOf(
                    mapOf<String, Any?>(
                        "op" to "add",
                        "path" to "/spec/replicas",
                        "value" to (resources.jobmanagerStatefulSet?.spec?.replicas ?: 1)
                    )
                )

                val response = appsApi.patchNamespacedStatefulSetScaleCall(
                    statefulSet.metadata.name,
                    clusterId.namespace,
                    V1Patch(Gson().toJson(patch)),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                ).execute()

                if (response.isSuccessful) {
                    logger.info("StatefulSet ${statefulSet.metadata.name} scaled")
                } else {
                    response.body().use { it.source().use { source -> logger.error(source.readUtf8Line()) } }
                    logger.warn("Can't scale StatefulSet ${statefulSet.metadata.name}")
                }
            } catch (e: Exception) {
                logger.warn("Failed to scale StatefulSet ${statefulSet.metadata.name}", e)
            }
        }
    }

    fun restartTaskManagerStatefulSets(
        clusterId: ClusterId,
        resources: ClusterResources
    ) {
        val statefulSets = appsApi.listNamespacedStatefulSet(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,role=taskmanager",
            null,
            null,
            5,
            null
        )

        statefulSets.items.forEach { statefulSet ->
            try {
                logger.info("Scaling StatefulSet ${statefulSet.metadata.name}...")

                val patch = listOf(
                    mapOf<String, Any?>(
                        "op" to "add",
                        "path" to "/spec/replicas",
                        "value" to (resources.taskmanagerStatefulSet?.spec?.replicas ?: 1)
                    )
                )

                val response = appsApi.patchNamespacedStatefulSetScaleCall(
                    statefulSet.metadata.name,
                    clusterId.namespace,
                    V1Patch(Gson().toJson(patch)),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                ).execute()

                if (response.isSuccessful) {
                    logger.info("StatefulSet ${statefulSet.metadata.name} scaled")
                } else {
                    response.body().use { it.source().use { source -> logger.error(source.readUtf8Line()) } }
                    logger.warn("Can't scale StatefulSet ${statefulSet.metadata.name}")
                }
            } catch (e: Exception) {
                logger.warn("Failed to scale StatefulSet ${statefulSet.metadata.name}", e)
            }
        }
    }

    fun terminateStatefulSets(clusterId: ClusterId) {
        val statefulSets = appsApi.listNamespacedStatefulSet(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator",
            null,
            null,
            5,
            null
        )

        statefulSets.items.forEach { statefulSet ->
            try {
                logger.info("Scaling StatefulSet ${statefulSet.metadata.name}...")

                val patch = listOf(
                    mapOf<String, Any?>(
                        "op" to "replace",
                        "path" to "/spec/replicas",
                        "value" to 0
                    )
                )

                val response = appsApi.patchNamespacedStatefulSetScaleCall(
                    statefulSet.metadata.name,
                    clusterId.namespace,
                    V1Patch(Gson().toJson(patch)),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                ).execute()

                if (response.isSuccessful) {
                    logger.info("StatefulSet ${statefulSet.metadata.name} scaled")
                } else {
                    response.body().use { it.source().use { source -> logger.error(source.readUtf8Line()) } }
                    logger.warn("Can't scale StatefulSet ${statefulSet.metadata.name}")
                }
            } catch (e: Exception) {
                logger.warn("Failed to scale StatefulSet ${statefulSet.metadata.name}", e)
            }
        }
    }

    fun deleteUploadJobPods(clusterId: ClusterId) {
        val pods = coreApi.listNamespacedPod(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,job-name=flink-upload-${clusterId.name}",
            null,
            null,
            5,
            null
        )

        val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

        pods.items.forEach { pod ->
            try {
                logger.info("Removing Job ${pod.metadata.name}...")

                val status = coreApi.deleteNamespacedPod(
                    pod.metadata.name,
                    clusterId.namespace,
                    null,
                    deleteOptions,
                    null,
                    5,
                    null,
                    null
                )

                logger.debug("Response status: ${status.reason}")

                status.details.causes.forEach { logger.info(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }
    }

    fun deleteUploadJobs(
        api: BatchV1Api,
        clusterId: ClusterId
    ) {
        val jobs = batchApi.listNamespacedJob(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,job-name=flink-upload-${clusterId.name}",
            null,
            null,
            5,
            null
        )

        val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

        jobs.items.forEach { job ->
            try {
                logger.info("Removing Job ${job.metadata.name}...")

                val status = batchApi.deleteNamespacedJob(
                    job.metadata.name,
                    clusterId.namespace,
                    null,
                    deleteOptions,
                    null,
                    5,
                    null,
                    null
                )

                logger.debug("Response status: ${status.reason}")

                status.details.causes.forEach { logger.info(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }
    }

    fun rescaleCluster(clusterId: ClusterId, taskManagers: Int) {
        val patch = mapOf<String, Any?>(
            "spec" to mapOf<String, Any?>(
                "replicas" to taskManagers
            )
        )

        val response = objectApi.patchNamespacedCustomObjectScaleCall(
            "nextbreakpoint.com",
            "v1",
            clusterId.namespace,
            "flinkclusters",
            clusterId.name,
            patch,
            null,
            null
        ).execute()

        if (!response.isSuccessful) {
            response.body().use { it.source().use { source -> logger.error(source.readUtf8Line()) } }
            throw RuntimeException("Can't modify scale of cluster ${clusterId.name}")
        }
    }

    fun setTaskManagerStatefulSetReplicas(clusterId: ClusterId, taskManagers: Int) {
        val statefulSets = appsApi.listNamespacedStatefulSet(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,role=taskmanager",
            null,
            null,
            5,
            null
        )

        if (statefulSets.items.size == 0) {
            throw RuntimeException("Can't find task managers of cluster ${clusterId.name}")
        }

        statefulSets.items.forEach { statefulSet ->
            try {
                logger.info("Scaling StatefulSet ${statefulSet.metadata.name}...")

                val patch = listOf(
                    mapOf<String, Any?>(
                        "op" to "replace",
                        "path" to "/spec/replicas",
                        "value" to taskManagers
                    )
                )

                val response = appsApi.patchNamespacedStatefulSetScaleCall(
                    statefulSet.metadata.name,
                    clusterId.namespace,
                    V1Patch(Gson().toJson(patch)),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                ).execute()

                if (response.isSuccessful) {
                    logger.info("StatefulSet ${statefulSet.metadata.name} scaled")
                } else {
                    response.body().use { it.source().use { source -> logger.error(source.readUtf8Line()) } }
                    logger.warn("Can't scale StatefulSet ${statefulSet.metadata.name}")
                }
            } catch (e: Exception) {
                logger.warn("Failed to scale StatefulSet ${statefulSet.metadata.name}", e)
            }
        }
    }

    fun getTaskManagerStatefulSetReplicas(clusterId: ClusterId): Int {
        val statefulSets = appsApi.listNamespacedStatefulSet(
            clusterId.namespace,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,role=taskmanager",
            null,
            null,
            5,
            null
        )

        if (statefulSets.items.size == 0) {
            throw RuntimeException("Can't find task managers of cluster ${clusterId.name}")
        }

        return statefulSets.items.firstOrNull()?.status?.currentReplicas ?: 0
    }

    @ExperimentalCoroutinesApi
    @Throws(InterruptedException::class)
    fun forwardPort(
        pod: V1Pod?,
        localPort: Int,
        port: Int,
        stop: Channel<Int>
    ): Thread {
        return Thread(
            Runnable {
                var stdout : Thread? = null
                var stdin : Thread? = null
                try {
                    val forwardResult = PortForward().forward(pod, listOf(port))
                    val serverSocket = ServerSocket(localPort)
                    val clientSocket = serverSocket.accept()
                    stop.invokeOnClose {
                        try {
                            clientSocket.close()
                        } catch (e: Exception) {
                        }
                        try {
                            serverSocket.close()
                        } catch (e: Exception) {
                        }
                    }
                    stdout = Thread(
                        Runnable {
                            try {
                                ByteStreams.copy(clientSocket.inputStream, forwardResult.getOutboundStream(port))
                            } catch (ex: Exception) {
                            }
                        })
                    stdin = Thread(
                        Runnable {
                            try {
                                ByteStreams.copy(forwardResult.getInputStream(port), clientSocket.outputStream)
                            } catch (ex: Exception) {
                            }
                        })
                    stdout.start()
                    stdin.start()
                    stdout.join()
                    stdin.interrupt()
                    stdin.join()
                    stdout = null
                    stdin = null
                } catch (e: Exception) {
                    stdout?.interrupt()
                    stdin?.interrupt()
                    logger.error("An error occurred", e)
                } finally {
                    stdout?.join()
                    stdin?.join()
                }
            })
    }

    @Throws(InterruptedException::class)
    fun processExec(proc: Process) {
        var stdout : Thread? = null
        var stderr : Thread? = null
        try {
            stdout = Thread(
                Runnable {
                    try {
                        ByteStreams.copy(proc.inputStream, System.out)
                    } catch (ex: Exception) {
                    }
                })
            stderr = Thread(
                Runnable {
                    try {
                        ByteStreams.copy(proc.errorStream, System.out)
                    } catch (ex: Exception) {
                    }
                })
            stdout.start()
            stderr.start()
            proc.waitFor(60, TimeUnit.SECONDS)
            stdout.join()
            stderr.join()
            stdout = null
            stderr = null
        } catch (e: Exception) {
            stdout?.interrupt()
            stderr?.interrupt()
            logger.error("An error occurred", e)
        } finally {
            stdout?.join()
            stderr?.join()
        }
    }

    private fun createKubernetesClient(kubeConfig: String?, timeout: Long): ApiClient? {
        val client = if (kubeConfig?.isNotBlank() == true) Config.fromConfig(FileInputStream(File(kubeConfig))) else Config.fromCluster()
        client.httpClient.setConnectTimeout(10000, TimeUnit.MILLISECONDS)
        client.httpClient.setWriteTimeout(timeout, TimeUnit.MILLISECONDS)
        client.httpClient.setReadTimeout(timeout, TimeUnit.MILLISECONDS)
        client.isDebugging = System.getProperty("kubernetes.client.debugging", "false")!!.toBoolean()
        return client
    }
}
