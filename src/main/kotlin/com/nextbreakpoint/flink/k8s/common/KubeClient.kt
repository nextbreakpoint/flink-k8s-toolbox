package com.nextbreakpoint.flink.k8s.common

import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.DeleteOptions
import com.nextbreakpoint.flink.common.FlinkAddress
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobStatus
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterStatus
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.apis.AppsV1Api
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.apis.CustomObjectsApi
import io.kubernetes.client.custom.V1Patch
import io.kubernetes.client.openapi.models.V1DeleteOptions
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service
import io.kubernetes.client.util.Config
import io.kubernetes.client.util.PatchUtils
import io.kubernetes.client.util.Watch
import io.kubernetes.client.util.Watchable
import org.apache.log4j.Logger
import java.io.File
import java.io.FileInputStream

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

    fun findFlinkAddress(flinkOptions: FlinkOptions, namespace: String, clusterName: String): FlinkAddress {
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
                val response = objectApi.getNamespacedCustomObjectCall(
                    "nextbreakpoint.com",
                    "v2",
                    namespace,
                    "flinkclusters",
                    clusterName,
                    null
                ).execute()

                response.body()?.use { body ->
                    body.source().use { source ->
                        val line = source.readUtf8Line() ?: ""

                        if (!response.isSuccessful) {
                            logger.error(line)
                            throw NotFoundException("Can't fetch custom object $clusterName")
                        }

                        val flinkCluster = Resource.parseV2FlinkCluster(line)

                        val clusterId = flinkCluster.metadata.uid

                        val services = coreApi.listNamespacedService(
                            namespace,
                            null,
                            null,
                            null,
                            null,
                            "clusterName=$clusterName,clusterUid=$clusterId,role=jobmanager",
                            1,
                            null,
                            30,
                            null
                        )

                        if (services.items.isNotEmpty()) {
                            val service = services.items[0]

                            logger.debug("Found JobManager service ${service.metadata?.name}")

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
                        } else {
                            throw NotFoundException("JobManager service not found (name=$clusterName, id=$clusterId)")
                        }

                        val pods = coreApi.listNamespacedPod(
                            namespace,
                            null,
                            null,
                            null,
                            null,
                            "clusterName=$clusterName,clusterUid=$clusterId,role=jobmanager",
                            1,
                            null,
                            30,
                            null
                        )

                        if (pods.items.isNotEmpty()) {
                            val pod = pods.items[0]

                            logger.debug("Found JobManager pod ${pod.metadata?.name}")
                        } else {
                            throw NotFoundException("JobManager pod not found (name=$clusterName, id=$clusterId)")
                        }
                    }
                }
            }

            logger.debug("Flink client created for host $jobmanagerHost and port $jobmanagerPort")

            return FlinkAddress(
                host = jobmanagerHost,
                port = jobmanagerPort
            )
        } catch (e : ApiException) {
            logger.error("Can't locate flink cluster: ${e.responseBody}")
            throw e
        }
    }

    fun getFlinkJob(jobSelector: ResourceSelector): V1FlinkJob {
        try {
            val response = objectApi.getNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v1",
                jobSelector.namespace,
                "flinkjobs",
                jobSelector.name,
                null
            ).execute()

            response.body()?.use { body ->
                body.source().use { source ->
                    val line = source.readUtf8Line() ?: ""

                    if (!response.isSuccessful) {
                        logger.error(line)
                        throw NotFoundException("Can't fetch custom object ${jobSelector.name}")
                    }

                    return Resource.parseV1FlinkJob(line)
                }
            }

            throw NotFoundException("Flink job not found: ${jobSelector.name}")
        } catch (e : ApiException) {
            logger.error("Can't fetch custom object: ${e.responseBody}")
            throw e
        }
    }

    fun updateClusterAnnotations(clusterSelector: ResourceSelector, annotations: Map<String, String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "annotations" to annotations
            )
        )

        try {
            PatchUtils.patch(
                V2FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectCall(
                        "nextbreakpoint.com",
                        "v2",
                        clusterSelector.namespace,
                        "flinkclusters",
                        clusterSelector.name,
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
            logger.error("Can't update annotations of cluster ${clusterSelector.name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateJobAnnotations(jobSelector: ResourceSelector, annotations: Map<String, String>) {
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
                        jobSelector.namespace,
                        "flinkjobs",
                        jobSelector.name,
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
            logger.error("Can't update annotations of job ${jobSelector.name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateClusterFinalizers(clusterSelector: ResourceSelector, finalizers: List<String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "finalizers" to finalizers
            )
        )

        try {
            PatchUtils.patch(
                V2FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectCall(
                        "nextbreakpoint.com",
                        "v2",
                        clusterSelector.namespace,
                        "flinkclusters",
                        clusterSelector.name,
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
            logger.error("Can't update finalizers of cluster ${clusterSelector.name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateJobFinalizers(jobSelector: ResourceSelector, finalizers: List<String>) {
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
                        jobSelector.namespace,
                        "flinkjobs",
                        jobSelector.name,
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
            logger.error("Can't update finalizers of job ${jobSelector.name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateClusterStatus(clusterSelector: ResourceSelector, status: V2FlinkClusterStatus) {
        val patch = V2FlinkCluster().status(status)

        try {
            PatchUtils.patch(
                V2FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectStatusCall(
                        "nextbreakpoint.com",
                        "v2",
                        clusterSelector.namespace,
                        "flinkclusters",
                        clusterSelector.name,
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
            logger.error("Can't update status of cluster ${clusterSelector.name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateJobStatus(jobSelector: ResourceSelector, status: V1FlinkJobStatus) {
        val patch = V1FlinkJob().status(status)

        try {
            PatchUtils.patch(
                V1FlinkJob::class.java, {
                    objectApi.patchNamespacedCustomObjectStatusCall(
                        "nextbreakpoint.com",
                        "v1",
                        jobSelector.namespace,
                        "flinkjobs",
                        jobSelector.name,
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
            logger.error("Can't update status of job ${jobSelector.name}: ${e.responseBody}")
            throw e
        }
    }

    fun rescaleCluster(clusterSelector: ResourceSelector, taskManagers: Int) {
        val patch = mapOf<String, Any?>(
            "spec" to mapOf<String, Any?>(
                "replicas" to taskManagers
            )
        )

        try {
            PatchUtils.patch(
                V2FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectScaleCall(
                        "nextbreakpoint.com",
                        "v2",
                        clusterSelector.namespace,
                        "flinkclusters",
                        clusterSelector.name,
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
            logger.error("Can't modify task managers of cluster ${clusterSelector.name}: ${e.responseBody}")
            throw e
        }
    }

    fun rescaleJob(jobSelector: ResourceSelector, parallelism: Int) {
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
                        jobSelector.namespace,
                        "flinkjobs",
                        jobSelector.name,
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
            logger.error("Can't modify parallelism of job ${jobSelector.name}: ${e.responseBody}")
            throw e
        }
    }

    fun watchFlickClustersV1(namespace: String): Watchable<V1FlinkCluster> =
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
                null,
                null,
                600,
                true,
                null
            ),
            object : TypeToken<Watch.Response<V1FlinkCluster>>() {}.type
        )

    fun watchFlickClustersV2(namespace: String): Watchable<V2FlinkCluster> =
        Watch.createWatch(
            objectApiWatch.apiClient,
            objectApiWatch.listNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v2",
                namespace,
                "flinkclusters",
                null,
                null,
                null,
                null,
                null,
                null,
                600,
                true,
                null
            ),
            object : TypeToken<Watch.Response<V2FlinkCluster>>() {}.type
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
                null,
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
                "component=flink,owner=flink-operator,role=supervisor",
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
                "component=flink,owner=flink-operator,job=bootstrap",
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

    fun createService(
        clusterSelector: ResourceSelector,
        resource: V1Service
    ): V1Service {
        try {
             return coreApi.createNamespacedService(
                clusterSelector.namespace,
                resource,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun createPod(
        clusterSelector: ResourceSelector,
        resource: V1Pod
    ): V1Pod {
        try {
             return coreApi.createNamespacedPod(
                clusterSelector.namespace,
                resource,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun deleteService(clusterSelector: ResourceSelector) {
        try {
            val services = coreApi.listNamespacedService(
                clusterSelector.namespace,
                null,
                null,
                null,
                null,
                "clusterName=${clusterSelector.name},clusterUid=${clusterSelector.uid},owner=flink-operator",
                null,
                null,
                5,
                null
            )

            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            services.items.forEach { service ->
                try {
                    logger.debug("Removing Service ${service.metadata?.name}...")

                    coreApi.deleteNamespacedService(
                        service.metadata?.name,
                        service.metadata?.namespace,
                        null,
                        null,
                        5,
                        null,
                        null,
                        deleteOptions
                    )
                } catch (e: ApiException) {
                    logger.error(e)
                    // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
                }
            }
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun deletePod(clusterSelector: ResourceSelector, name: String) {
        try {
            logger.debug("Removing Pod ${name}...")

            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            coreApi.deleteNamespacedPod(
                name,
                clusterSelector.namespace,
                null,
                null,
                5,
                null,
                null,
                deleteOptions
            )
        } catch (e: ApiException) {
            logger.error(e)
            // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            throw e
        }
    }

    fun deletePods(clusterSelector: ResourceSelector, options: DeleteOptions) {
        try {
            val pods = coreApi.listNamespacedPod(
                clusterSelector.namespace,
                null,
                null,
                null,
                null,
                "clusterName=${clusterSelector.name},clusterUid=${clusterSelector.uid},owner=flink-operator,${options.label}=${options.value}",
                null,
                null,
                5,
                null
            )

            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            pods.items.take(options.limit).forEach { pod ->
                try {
                    logger.debug("Removing Pod ${pod.metadata?.name}...")

                    coreApi.deleteNamespacedPod(
                        pod.metadata?.name,
                        pod.metadata?.namespace,
                        null,
                        null,
                        5,
                        null,
                        null,
                        deleteOptions
                    )
                } catch (e: ApiException) {
                    logger.error(e)
                    // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
                }
            }
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun createFlinkClusterV1(flinkCluster: V1FlinkCluster) {
        try {
            objectApi.createNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                flinkCluster.metadata.namespace,
                "flinkclusters",
                flinkCluster,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun deleteFlinkClusterV1(clusterSelector: ResourceSelector) {
        try {
            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            objectApi.deleteNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                clusterSelector.namespace,
                "flinkclusters",
                clusterSelector.name,
                5,
                null,
                null,
                null,
                deleteOptions
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun createFlinkClusterV2(flinkCluster: V2FlinkCluster) {
        try {
            objectApi.createNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v2",
                flinkCluster.metadata.namespace,
                "flinkclusters",
                flinkCluster,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun deleteFlinkClusterV2(clusterSelector: ResourceSelector) {
        try {
            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            objectApi.deleteNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v2",
                clusterSelector.namespace,
                "flinkclusters",
                clusterSelector.name,
                5,
                null,
                null,
                null,
                deleteOptions
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun createFlinkJob(flinkJob: V1FlinkJob) {
        try {
            objectApi.createNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                flinkJob.metadata.namespace,
                "flinkjobs",
                flinkJob,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun deleteFlinkJob(jobSelector: ResourceSelector) {
        try {
            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            objectApi.deleteNamespacedCustomObjectWithHttpInfo(
                "nextbreakpoint.com",
                "v1",
                jobSelector.namespace,
                "flinkjobs",
                jobSelector.name,
                5,
                null,
                null,
                null,
                deleteOptions
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun createBootstrapJob(clusterSelector: ResourceSelector, bootstrapJob: V1Job): V1Job {
        try {
            return batchApi.createNamespacedJob(
                clusterSelector.namespace,
                bootstrapJob,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun deleteBootstrapJob(clusterSelector: ResourceSelector, jobName: String) {
        try {
            val jobs = batchApi.listNamespacedJob(
                clusterSelector.namespace,
                null,
                null,
                null,
                null,
                "clusterName=${clusterSelector.name},jobName=${jobName},owner=flink-operator,job=bootstrap",
                null,
                null,
                5,
                null
            )

            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            jobs.items.forEach { job ->
                try {
                    logger.debug("Removing Job ${job.metadata?.name}...")

                    batchApi.deleteNamespacedJob(
                        job.metadata?.name,
                        job.metadata?.namespace,
                        null,
                        null,
                        5,
                        null,
                        null,
                        deleteOptions
                    )
                } catch (e: ApiException) {
                    logger.error(e)
                    // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
                }
            }
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

//    fun listTaskManagerPods(clusterSelector: ResourceSelector): V1PodList {
//        try {
//            return coreApi.listNamespacedPod(
//                clusterSelector.namespace,
//                null,
//                null,
//                null,
//                null,
//                "clusterName=${clusterSelector.name},clusterUid=${clusterSelector.uid},owner=flink-operator,role=taskmanager",
//                null,
//                null,
//                5,
//                null
//            )
//        } catch (e : ApiException) {
//            logger.error(e.responseBody)
//            throw e
//        }
//    }
//
//    fun listJobManagerPods(clusterSelector: ResourceSelector): V1PodList {
//        try {
//            return coreApi.listNamespacedPod(
//                clusterSelector.namespace,
//                null,
//                null,
//                null,
//                null,
//                "clusterName=${clusterSelector.name},clusterUid=${clusterSelector.uid},owner=flink-operator,role=jobmanager",
//                null,
//                null,
//                5,
//                null
//            )
//        } catch (e : ApiException) {
//            logger.error(e.responseBody)
//            throw e
//        }
//    }

    fun deleteBootstrapPod(jobSelector: ResourceSelector, params: String) {
        try {
            val tokens = jobSelector.name.split("-")
            val clusterName = tokens[0]
            val jobName = tokens[1]

            val pods = coreApi.listNamespacedPod(
                jobSelector.namespace,
                null,
                null,
                null,
                null,
                "clusterName=${clusterName},jobName=${jobName},owner=flink-operator,job=bootstrap",
                null,
                null,
                5,
                null
            )

            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            pods.items.forEach { pod ->
                try {
                    logger.debug("Removing Pod ${pod.metadata?.name}...")

                    coreApi.deleteNamespacedPod(
                        pod.metadata?.name,
                        pod.metadata?.namespace,
                        null,
                        null,
                        5,
                        null,
                        null,
                        deleteOptions
                    )
                } catch (e: ApiException) {
                    logger.error(e)
                    // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
                }
            }
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun createSupervisorDeployment(clusterSelector: ResourceSelector, resource: V1Deployment): V1Deployment {
        try {
            return appsApi.createNamespacedDeployment(
                clusterSelector.namespace,
                resource,
                null,
                null,
                null
            )
        } catch (e : ApiException) {
            logger.error(e.responseBody)
            throw e
        }
    }

    fun deleteSupervisorDeployment(clusterSelector: ResourceSelector) {
        try {
            val deployments = appsApi.listNamespacedDeployment(
                clusterSelector.namespace,
                null,
                null,
                null,
                null,
                "clusterName=${clusterSelector.name},clusterUid=${clusterSelector.uid},owner=flink-operator,role=supervisor",
                null,
                null,
                5,
                null
            )

            val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

            deployments.items.forEach { service ->
                try {
                    logger.debug("Removing Deployment ${service.metadata?.name}...")

                    appsApi.deleteNamespacedDeployment(
                        service.metadata?.name,
                        service.metadata?.namespace,
                        null,
                        null,
                        5,
                        null,
                        null,
                        deleteOptions
                    )
                } catch (e: ApiException) {
                    logger.error(e)
                    // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
                }
            }
        } catch (e : ApiException) {
            logger.error(e.responseBody)
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
