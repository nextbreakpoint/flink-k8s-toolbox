package com.nextbreakpoint.flink.k8s.common

import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flink.common.FlinkAddress
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeploymentStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobStatus
import io.kubernetes.client.custom.V1Patch
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.apis.AppsV1Api
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.apis.CustomObjectsApi
import io.kubernetes.client.openapi.models.V1DeleteOptions
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service
import io.kubernetes.client.util.Config
import io.kubernetes.client.util.PatchUtils
import io.kubernetes.client.util.Watch
import io.kubernetes.client.util.Watchable
import java.io.File
import java.io.FileInputStream
import java.util.logging.Level
import java.util.logging.Logger

object KubeClient {
    private val logger = Logger.getLogger(KubeClient::class.simpleName)

    private val objectApi = CustomObjectsApi()
    private val batchApi = BatchV1Api()
    private val coreApi = CoreV1Api()
    private val appsApi = AppsV1Api()

    private val objectApiWatch = CustomObjectsApi()
    private val batchApiWatch = BatchV1Api()
    private val coreApiWatch = CoreV1Api()
    private val appsApiWatch = AppsV1Api()

    fun configure(kubeConfig: String?) {
        Configuration.setDefaultApiClient(createKubernetesApiClient(kubeConfig, 5000))
        objectApi.apiClient = Configuration.getDefaultApiClient()
        batchApi.apiClient = Configuration.getDefaultApiClient()
        coreApi.apiClient = Configuration.getDefaultApiClient()
        appsApi.apiClient = Configuration.getDefaultApiClient()
        objectApiWatch.apiClient = createKubernetesApiClient(kubeConfig, 0)
        batchApiWatch.apiClient = createKubernetesApiClient(kubeConfig, 0)
        coreApiWatch.apiClient = createKubernetesApiClient(kubeConfig, 0)
        appsApiWatch.apiClient = createKubernetesApiClient(kubeConfig, 0)
    }

    fun findFlinkAddress(flinkOptions: FlinkOptions, namespace: String, name: String): FlinkAddress {
        try {
            var jobmanagerHost = flinkOptions.hostname ?: "localhost"
            var jobmanagerPort = flinkOptions.portForward ?: 8081

            if (flinkOptions.hostname == null && flinkOptions.portForward == null && flinkOptions.useNodePort) {
                val nodes = coreApi.listNode(
                    null,
                    null,
                    null,
                    null,
                    null,
                    1,
                    null,
                    30,
                    null
                )

                jobmanagerHost = nodes.items.flatMap {
                    it.status?.addresses.orEmpty()
                }.filter {
                    it.type == "InternalIP"
                }.map {
                    it.address
                }.firstOrNull() ?: throw NotFoundException("Node not found")
            }

            if (flinkOptions.portForward == null) {
                val service = coreApi.readNamespacedService(
                    "jobmanager-$name",
                    namespace,
                    null,
                    null,
                    null,
                )

                if (flinkOptions.useNodePort) {
                    service.spec?.ports?.filter {
                        it.name.equals("ui")
                    }?.filter {
                        it.nodePort != null
                    }?.map {
                        it.nodePort
                    }?.firstOrNull()?.let {
                        jobmanagerPort = it
                    }
                } else {
                    service.spec?.ports?.filter {
                        it.name.equals("ui")
                    }?.filter {
                        it.port != null
                    }?.map {
                        it.port
                    }?.firstOrNull()?.let {
                        jobmanagerPort = it
                    }
                    jobmanagerHost = service.spec?.clusterIP ?: ""
                }
            }

            logger.log(Level.FINE, "Flink client created for host $jobmanagerHost and port $jobmanagerPort")

            return FlinkAddress(
                host = jobmanagerHost,
                port = jobmanagerPort
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't connect to jobmanager: ${e.responseBody}")
            throw e
        }
    }

    fun getFlinkDeployment(namespace: String, name: String): V1FlinkDeployment {
        try {
            val response = objectApi.getNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkdeployments",
                name,
                null
            ).execute()

            response.body()?.use { body ->
                body.source().use { source ->
                    val line = source.readUtf8Line() ?: ""

                    if (!response.isSuccessful) {
                        logger.log(Level.SEVERE, line)
                        throw NotFoundException("Can't fetch custom object $name")
                    }

                    return Resource.parseV1FlinkDeployment(line)
                }
            }

            throw NotFoundException("Flink deployment not found: $name")
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't fetch custom object: ${e.responseBody}")
            throw e
        }
    }

    fun getFlinkCluster(namespace: String, name: String): V1FlinkCluster {
        try {
            val response = objectApi.getNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
                name,
                null
            ).execute()

            response.body()?.use { body ->
                body.source().use { source ->
                    val line = source.readUtf8Line() ?: ""

                    if (!response.isSuccessful) {
                        logger.log(Level.SEVERE, line)
                        throw NotFoundException("Can't fetch custom object ${name}")
                    }

                    return Resource.parseV1FlinkCluster(line)
                }
            }

            throw NotFoundException("Flink cluster not found: ${name}")
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't fetch custom object: ${e.responseBody}")
            throw e
        }
    }

    fun getFlinkJob(namespace: String, name: String): V1FlinkJob {
        try {
            val response = objectApi.getNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkjobs",
                name,
                null
            ).execute()

            response.body()?.use { body ->
                body.source().use { source ->
                    val line = source.readUtf8Line() ?: ""

                    if (!response.isSuccessful) {
                        logger.log(Level.SEVERE, line)
                        throw NotFoundException("Can't fetch custom object ${name}")
                    }

                    return Resource.parseV1FlinkJob(line)
                }
            }

            throw NotFoundException("Flink job not found: ${name}")
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't fetch custom object: ${e.responseBody}")
            throw e
        }
    }

    fun updateDeploymentAnnotations(namespace: String, name: String, annotations: Map<String, String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "annotations" to annotations
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkDeployment::class.java, {
                    objectApi.patchNamespacedCustomObjectCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkdeployments",
                        name,
                        patch,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update annotations of deployment ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateClusterAnnotations(namespace: String, name: String, annotations: Map<String, String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "annotations" to annotations
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkclusters",
                        name,
                        patch,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update annotations of cluster ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateJobAnnotations(namespace: String, name: String, annotations: Map<String, String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "annotations" to annotations
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkJob::class.java, {
                    objectApi.patchNamespacedCustomObjectCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkjobs",
                        name,
                        patch,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update annotations of job ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateDeploymentFinalizers(namespace: String, name: String, finalizers: List<String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "finalizers" to finalizers
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkDeployment::class.java, {
                    objectApi.patchNamespacedCustomObjectCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkdeployments",
                        name,
                        patch,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update finalizers of deployment ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateClusterFinalizers(namespace: String, name: String, finalizers: List<String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "finalizers" to finalizers
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkclusters",
                        name,
                        patch,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update finalizers of cluster ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateJobFinalizers(namespace: String, name: String, finalizers: List<String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "finalizers" to finalizers
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkJob::class.java, {
                    objectApi.patchNamespacedCustomObjectCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkjobs",
                        name,
                        patch,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update finalizers of job ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateDeploymentStatus(namespace: String, name: String, status: V1FlinkDeploymentStatus) {
        val patch = V1FlinkDeployment.builder().withStatus(status).build()

        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectStatusCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkdeployments",
                        name,
                        patch,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update status of deployment ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateClusterStatus(namespace: String, name: String, status: V1FlinkClusterStatus) {
        val patch = V1FlinkCluster.builder().withStatus(status).build()

        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectStatusCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkclusters",
                        name,
                        patch,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update status of cluster ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateJobStatus(namespace: String, name: String, status: V1FlinkJobStatus) {
        val patch = V1FlinkJob.builder().withStatus(status).build()

        try {
            PatchUtils.patch(
                V1FlinkJob::class.java, {
                    objectApi.patchNamespacedCustomObjectStatusCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkjobs",
                        name,
                        patch,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update status of job ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun rescaleCluster(namespace: String, name: String, taskManagers: Int) {
        val patch = mapOf<String, Any?>(
            "spec" to mapOf<String, Any?>(
                "replicas" to taskManagers
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectScaleCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkclusters",
                        name,
                        patch,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't modify task managers of cluster ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun rescaleJob(namespace: String, name: String, parallelism: Int) {
        val patch = mapOf<String, Any?>(
            "spec" to mapOf<String, Any?>(
                "replicas" to parallelism
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkJob::class.java, {
                    objectApi.patchNamespacedCustomObjectScaleCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkjobs",
                        name,
                        patch,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't modify parallelism of job ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun watchFlickDeployments(namespace: String): Watchable<V1FlinkDeployment> =
        Watch.createWatch(
            objectApiWatch.apiClient,
            objectApiWatch.listNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkdeployments",
                null,
                null,
                null,
                null,//"component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null
            ),
            object : TypeToken<Watch.Response<V1FlinkDeployment>>() {}.type
        )

    fun watchFlickClusters(namespace: String): Watchable<V1FlinkCluster> =
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
                null,//"component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null
            ),
            object : TypeToken<Watch.Response<V1FlinkCluster>>() {}.type
        )

    fun watchFlinkJobs(namespace: String): Watchable<V1FlinkJob> =
        Watch.createWatch(
            objectApiWatch.apiClient,
            objectApiWatch.listNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkjobs",
                null,
                null,
                null,
                null,//"component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null
            ),
            object : TypeToken<Watch.Response<V1FlinkJob>>() {}.type
        )

    fun watchServices(namespace: String): Watchable<V1Service> =
        Watch.createWatch(
            coreApiWatch.apiClient,
            coreApiWatch.listNamespacedServiceCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null
            ),
            object : TypeToken<Watch.Response<V1Service>>() {}.type
        )

    fun watchDeployments(namespace: String): Watchable<V1Deployment> =
        Watch.createWatch(
            appsApiWatch.apiClient,
            appsApiWatch.listNamespacedDeploymentCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null
            ),
            object : TypeToken<Watch.Response<V1Deployment>>() {}.type
        )

    fun watchJobs(namespace: String): Watchable<V1Job> =
        Watch.createWatch(
            batchApiWatch.apiClient,
            batchApiWatch.listNamespacedJobCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null
            ),
            object : TypeToken<Watch.Response<V1Job>>() {}.type
        )

    fun watchPods(namespace: String): Watchable<V1Pod> =
        Watch.createWatch(
            coreApiWatch.apiClient,
            coreApiWatch.listNamespacedPodCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null
            ),
            object : TypeToken<Watch.Response<V1Pod>>() {}.type
        )

    fun createService(namespace: String, resource: V1Service): V1Service {
        try {
             return coreApi.createNamespacedService(
                namespace,
                resource,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create service: ${e.responseBody}")
            throw e
        }
    }

    fun deleteService(namespace: String, name: String) {
        try {
            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            coreApi.deleteNamespacedService(
                name,
                namespace,
                null,
                null,
                5,
                null,
                null,
                deleteOptions
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete service: ${e.responseBody}")
            throw e
        }
    }

    fun createPod(namespace: String, resource: V1Pod): V1Pod {
        try {
            return coreApi.createNamespacedPod(
                namespace,
                resource,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create pod: ${e.responseBody}")
            throw e
        }
    }

    fun deletePod(namespace: String, name: String) {
        try {
            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            coreApi.deleteNamespacedPod(
                name,
                namespace,
                null,
                null,
                5,
                null,
                null,
                deleteOptions
            )
        } catch (e: ApiException) {
            logger.log(Level.SEVERE, "Can't delete pod: ${e.responseBody}")
            throw e
        }
    }

    fun createFlinkDeployment(namespace: String, resource: V1FlinkDeployment) {
        try {
            objectApi.createNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkdeployments",
                resource,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create flinkdeployment: ${e.responseBody}")
            throw e
        }
    }

    fun deleteFlinkDeployment(namespace: String, name: String) {
        try {
            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            objectApi.deleteNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkdeployments",
                name,
                5,
                null,
                null,
                null,
                deleteOptions
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete flinkdeployment: ${e.responseBody}")
            throw e
        }
    }

    fun updateFlinkDeployment(namespace: String, resource: V1FlinkDeployment) {
        try {
            PatchUtils.patch(
                V1FlinkDeployment::class.java, {
                    objectApi.patchNamespacedCustomObjectCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkdeployments",
                        resource.metadata.name,
                        resource,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update flinkdeployment: ${e.responseBody}")
            throw e
        }
    }

    fun createFlinkCluster(namespace: String, resource: V1FlinkCluster) {
        try {
            objectApi.createNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
                resource,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create flinkcluster: ${e.responseBody}")
            throw e
        }
    }

    fun deleteFlinkCluster(namespace: String, name: String) {
        try {
            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            objectApi.deleteNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
                name,
                5,
                null,
                null,
                null,
                deleteOptions
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete flinkcluster: ${e.responseBody}")
            throw e
        }
    }

    fun updateFlinkCluster(namespace: String, resource: V1FlinkCluster) {
        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkclusters",
                        resource.metadata.name,
                        resource,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update flinkcluster: ${e.responseBody}")
            throw e
        }
    }

    fun createFlinkJob(namespace: String, resource: V1FlinkJob) {
        try {
            objectApi.createNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkjobs",
                resource,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create flinkjob: ${e.responseBody}")
            throw e
        }
    }

    fun deleteFlinkJob(namespace: String, name: String) {
        try {
            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            objectApi.deleteNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkjobs",
                name,
                5,
                null,
                null,
                null,
                deleteOptions
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete flinkjob: ${e.responseBody}")
            throw e
        }
    }

    fun updateFlinkJob(namespace: String, resource: V1FlinkJob) {
        try {
            PatchUtils.patch(
                V1FlinkJob::class.java, {
                    objectApi.patchNamespacedCustomObjectCall(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkjobs",
                        resource.metadata.name,
                        resource,
                        null,
                        null,
                        null,
                        null
                    )
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update flinkjob: ${e.responseBody}")
            throw e
        }
    }

    fun createJob(namespace: String, resource: V1Job): V1Job {
        try {
            return batchApi.createNamespacedJob(
                namespace,
                resource,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create job: ${e.responseBody}")
            throw e
        }
    }

    fun deleteJob(namespace: String, name: String) {
        try {
            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            batchApi.deleteNamespacedJob(
                name,
                namespace,
                null,
                null,
                5,
                null,
                null,
                deleteOptions
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete job: ${e.responseBody}")
            throw e
        }
    }

    fun createDeployment(namespace: String, resource: V1Deployment): V1Deployment {
        try {
            return appsApi.createNamespacedDeployment(
                namespace,
                resource,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create deployment: ${e.responseBody}")
            throw e
        }
    }

    fun deleteDeployment(namespace: String, name: String) {
        try {
            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            appsApi.deleteNamespacedDeployment(
                name,
                namespace,
                null,
                null,
                5,
                null,
                null,
                deleteOptions
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete deployment: ${e.responseBody}")
            throw e
        }
    }

    private fun createKubernetesApiClient(kubeConfig: String?, timeout: Int): ApiClient? {
        val client = if (kubeConfig?.isNotBlank() == true) Config.fromConfig(FileInputStream(File(kubeConfig))) else Config.fromCluster()
        client.connectTimeout = timeout
        client.writeTimeout = timeout
        client.readTimeout = timeout
        client.isDebugging = System.getProperty("kubernetes.client.debugging", "false")!!.toBoolean()
        return client
    }
}
