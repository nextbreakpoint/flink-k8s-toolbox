package com.nextbreakpoint.command

import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.handler.ClusterCreateHandler
import com.nextbreakpoint.handler.ClusterDeleteHandler
import com.nextbreakpoint.model.ClusterConfig
import com.nextbreakpoint.model.ClusterDescriptor
import com.nextbreakpoint.model.JobManagerConfig
import com.nextbreakpoint.model.ResourcesConfig
import com.nextbreakpoint.model.OperatorConfig
import com.nextbreakpoint.model.SidecarConfig
import com.nextbreakpoint.model.StorageConfig
import com.nextbreakpoint.model.TaskManagerConfig
import com.nextbreakpoint.model.V1FlinkCluster
import com.nextbreakpoint.model.V1FlinkClusterSpec
import io.kubernetes.client.Configuration
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.apis.CustomObjectsApi
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1ObjectMeta
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
import io.kubernetes.client.util.Watch
import org.apache.log4j.Logger
import java.net.SocketTimeoutException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

class RunOperator {
    companion object {
        val logger = Logger.getLogger(RunOperator::class.simpleName)
    }

    private val sharedLock = Semaphore(1)
    private val queue = LinkedBlockingQueue<String>()
    private val clusters = mutableMapOf<ClusterDescriptor, V1FlinkCluster>()
    private val status = mutableMapOf<ClusterDescriptor, Long>()
    private val services = mutableMapOf<ClusterDescriptor, V1Service>()
    private val deployments = mutableMapOf<ClusterDescriptor, V1Deployment>()
    private val jobmanagerStatefulSets = mutableMapOf<ClusterDescriptor, V1StatefulSet>()
    private val taskmanagerStatefulSets = mutableMapOf<ClusterDescriptor, V1StatefulSet>()
    private val jobmanagerPersistentVolumeClaims = mutableMapOf<ClusterDescriptor, V1PersistentVolumeClaim>()
    private val taskmanagerPersistentVolumeClaims = mutableMapOf<ClusterDescriptor, V1PersistentVolumeClaim>()

    fun run(config: OperatorConfig) {
        RunController.logger.info("Launching operator...")

        val objectApi = CustomObjectsApi()

        val coreApi = CoreV1Api()

        val appsApi = AppsV1Api()

        thread {
            while (true) {
                try {
                    val watch = watchFlickClusterResources(config.namespace, objectApi)

                    watch.forEach { resource ->
                        val clusterName = resource.`object`.spec.clusterName
                        val environment = resource.`object`.spec.environment
                        if (clusterName != null && environment != null) {
                            sharedLock.acquire()
                            when (resource.type) {
                                "ADDED", "MODIFIED" -> clusters.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                "DELETED" -> clusters.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                            }
                            queue.add(resource.`object`.kind)
                            sharedLock.release()
                        }
                    }
                } catch (e : InterruptedException) {
                    break
                } catch (e : RuntimeException) {
                    if (!(e.cause is SocketTimeoutException)) {
                        e.printStackTrace()
                    }
                } catch (e : Exception) {
                    e.printStackTrace()
                }
            }
        }

        thread {
            while (true) {
                try {
                    val watch = watchServiceResources(config.namespace, coreApi)

                    watch.forEach { resource ->
                        val clusterName = resource.`object`.metadata.labels.get("cluster")
                        val environment = resource.`object`.metadata.labels.get("environment")
                        if (clusterName != null && environment != null) {
                            sharedLock.acquire()
                            when (resource.type) {
                                "ADDED", "MODIFIED" -> services.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                "DELETED" -> services.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                            }
                            queue.add(resource.`object`.kind)
                            sharedLock.release()
                        }
                    }
                } catch (e : InterruptedException) {
                    break
                } catch (e : RuntimeException) {
                    if (!(e.cause is SocketTimeoutException)) {
                        e.printStackTrace()
                    }
                } catch (e : Exception) {
                    e.printStackTrace()
                }
            }
        }

        thread {
            while (true) {
                try {
                    val watch = watchDeploymentResources(config.namespace, appsApi)

                    watch.forEach { resource ->
                        val clusterName = resource.`object`.metadata.labels.get("cluster")
                        val environment = resource.`object`.metadata.labels.get("environment")
                        if (clusterName != null && environment != null) {
                            sharedLock.acquire()
                            when (resource.type) {
                                "ADDED", "MODIFIED" -> deployments.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                "DELETED" -> deployments.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                            }
                            queue.add(resource.`object`.kind)
                            sharedLock.release()
                        }
                    }
                } catch (e : InterruptedException) {
                    break
                } catch (e : RuntimeException) {
                    if (!(e.cause is SocketTimeoutException)) {
                        e.printStackTrace()
                    }
                } catch (e : Exception) {
                    e.printStackTrace()
                }
            }
        }

        thread {
            while (true) {
                try {
                    val watch = watchStatefulSetResources(config.namespace, appsApi)

                    watch.forEach { resource ->
                        val clusterName = resource.`object`.metadata.labels.get("cluster")
                        val environment = resource.`object`.metadata.labels.get("environment")
                        val role = resource.`object`.metadata.labels.get("role")
                        if (clusterName != null && environment != null && role != null) {
                            sharedLock.acquire()
                            if (role.equals("jobmanager")) {
                                when (resource.type) {
                                    "ADDED", "MODIFIED" -> jobmanagerStatefulSets.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                    "DELETED" -> jobmanagerStatefulSets.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                                }
                            } else {
                                when (resource.type) {
                                    "ADDED", "MODIFIED" -> taskmanagerStatefulSets.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                    "DELETED" -> taskmanagerStatefulSets.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                                }
                            }
                            queue.add(resource.`object`.kind)
                            sharedLock.release()
                        }
                    }
                } catch (e : InterruptedException) {
                    break
                } catch (e : RuntimeException) {
                    if (!(e.cause is SocketTimeoutException)) {
                        e.printStackTrace()
                    }
                } catch (e : Exception) {
                    e.printStackTrace()
                }
            }
        }

        thread {
            while (true) {
                try {
                    val watch = watchPermanentVolumeClaimResources(config.namespace, coreApi)

                    watch.forEach { resource ->
                        val clusterName = resource.`object`.metadata.labels.get("cluster")
                        val environment = resource.`object`.metadata.labels.get("environment")
                        val role = resource.`object`.metadata.labels.get("role")
                        if (clusterName != null && environment != null && role != null) {
                            sharedLock.acquire()
                            if (role.equals("jobmanager")) {
                                when (resource.type) {
                                    "ADDED", "MODIFIED" -> jobmanagerPersistentVolumeClaims.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                    "DELETED" -> jobmanagerPersistentVolumeClaims.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                                }
                            } else {
                                when (resource.type) {
                                    "ADDED", "MODIFIED" -> taskmanagerPersistentVolumeClaims.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                    "DELETED" -> taskmanagerPersistentVolumeClaims.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                                }
                            }
                            queue.add(resource.`object`.kind)
                            sharedLock.release()
                        }
                    }
                } catch (e : InterruptedException) {
                    break
                } catch (e : RuntimeException) {
                    if (!(e.cause is SocketTimeoutException)) {
                        e.printStackTrace()
                    }
                } catch (e : Exception) {
                    e.printStackTrace()
                }
            }
        }

        try {
            Thread.sleep(10000L)

            while (true) {
                logger.info("Wait for next event...")

                val values = queue.poll(60, TimeUnit.SECONDS)

                if (values != null) {
                    while (!queue.isEmpty()) {
                        queue.remove()
                    }

                    reconcile()
                }

                Thread.sleep(5000L)
            }
        } catch (e: Exception) {
            logger.error("An error occurred while processing the resources", e)
        }
    }

    private fun reconcile() {
        sharedLock.acquire()

        try {
            logger.info("Found ${clusters.size} Flink Cluster resource${if (clusters.size == 1) "" else "s"}")

            val divergentClusters = mutableMapOf<ClusterDescriptor, ClusterConfig>()

            clusters.values.forEach { cluster ->
                val clusterConfig = createClusterConfig(cluster.metadata, cluster.spec)

                if (hasDiverged(
                        clusterConfig,
                        deployments,
                        jobmanagerStatefulSets,
                        taskmanagerStatefulSets,
                        services,
                        jobmanagerPersistentVolumeClaims,
                        taskmanagerPersistentVolumeClaims
                    )
                ) {
                    val lastUpdated = status.get(clusterConfig.descriptor)

                    if (lastUpdated == null) {
                        logger.info("Cluster ${clusterConfig.descriptor.name} has diverged. Reconciling state...")

                        divergentClusters.put(clusterConfig.descriptor, clusterConfig)

                        status.put(clusterConfig.descriptor, System.currentTimeMillis())
                    } else if (System.currentTimeMillis() - lastUpdated > 120000) {
                        logger.info("Cluster ${clusterConfig.descriptor.name} has diverged. Reconciling state...")

                        divergentClusters.put(clusterConfig.descriptor, clusterConfig)

                        status.put(clusterConfig.descriptor, System.currentTimeMillis())
                    }
                }
            }

            clusters.values.forEach { cluster ->
                val clusterConfig = createClusterConfig(cluster.metadata, cluster.spec)

                if (divergentClusters.containsKey(clusterConfig.descriptor)) {
                    logger.info("Deleting cluster ${clusterConfig.descriptor.name}...")

                    ClusterDeleteHandler.execute(clusterConfig.descriptor)
                }
            }

            clusters.values.forEach { cluster ->
                val clusterConfig = createClusterConfig(cluster.metadata, cluster.spec)

                if (divergentClusters.containsKey(clusterConfig.descriptor)) {
                    logger.info("Creating cluster ${clusterConfig.descriptor.name}...")

                    ClusterCreateHandler.execute("flink-operator", clusterConfig)
                }
            }

            val clusterConfigs = mutableMapOf<ClusterDescriptor, ClusterConfig>()

            clusters.values.forEach { cluster ->
                val clusterConfig = createClusterConfig(cluster.metadata, cluster.spec)

                clusterConfigs.put(clusterConfig.descriptor, clusterConfig)
            }

            deleteOrphans(
                services,
                clusterConfigs,
                deployments,
                jobmanagerStatefulSets,
                taskmanagerStatefulSets,
                jobmanagerPersistentVolumeClaims,
                taskmanagerPersistentVolumeClaims
            )
        } finally {
            sharedLock.release()
        }
    }

    private fun deleteOrphans(
        services: MutableMap<ClusterDescriptor, V1Service>,
        flinkClusters: MutableMap<ClusterDescriptor, ClusterConfig>,
        deployments: MutableMap<ClusterDescriptor, V1Deployment>,
        jobmanagerStatefulSets: MutableMap<ClusterDescriptor, V1StatefulSet>,
        taskmanagerStatefulSets: MutableMap<ClusterDescriptor, V1StatefulSet>,
        jobmanagerPersistentVolumeClaims: MutableMap<ClusterDescriptor, V1PersistentVolumeClaim>,
        taskmanagerPersistentVolumeClaims: MutableMap<ClusterDescriptor, V1PersistentVolumeClaim>
    ) {
        val pendingDeleteClusters = mutableMapOf<ClusterDescriptor, ClusterDescriptor>()

        services.forEach { descriptor, service ->
            if (pendingDeleteClusters.get(descriptor) == null && flinkClusters.get(descriptor) == null) {
                logger.info("Deleting orphan cluster ${descriptor.name}...")

                ClusterDeleteHandler.execute(descriptor)

                pendingDeleteClusters.put(descriptor, descriptor)
            }
        }

        deployments.forEach { descriptor, deployment ->
            if (pendingDeleteClusters.get(descriptor) == null && flinkClusters.get(descriptor) == null) {
                logger.info("Deleting orphan cluster ${descriptor.name}...")

                ClusterDeleteHandler.execute(descriptor)

                pendingDeleteClusters.put(descriptor, descriptor)
            }
        }

        jobmanagerStatefulSets.forEach { descriptor, statefulSet ->
            if (pendingDeleteClusters.get(descriptor) == null && flinkClusters.get(descriptor) == null) {
                logger.info("Deleting orphan cluster ${descriptor.name}...")

                ClusterDeleteHandler.execute(descriptor)

                pendingDeleteClusters.put(descriptor, descriptor)
            }
        }

        taskmanagerStatefulSets.forEach { descriptor, statefulSet ->
            if (pendingDeleteClusters.get(descriptor) == null && flinkClusters.get(descriptor) == null) {
                logger.info("Deleting orphan cluster ${descriptor.name}...")

                ClusterDeleteHandler.execute(descriptor)

                pendingDeleteClusters.put(descriptor, descriptor)
            }
        }

        jobmanagerPersistentVolumeClaims.forEach { descriptor, persistentVolumeClaims ->
            if (pendingDeleteClusters.get(descriptor) == null && flinkClusters.get(descriptor) == null) {
                logger.info("Deleting orphan cluster ${descriptor.name}...")

                ClusterDeleteHandler.execute(descriptor)

                pendingDeleteClusters.put(descriptor, descriptor)
            }
        }

        taskmanagerPersistentVolumeClaims.forEach { descriptor, persistentVolumeClaims ->
            if (pendingDeleteClusters.get(descriptor) == null && flinkClusters.get(descriptor) == null) {
                logger.info("Deleting orphan cluster ${descriptor.name}...")

                ClusterDeleteHandler.execute(descriptor)

                pendingDeleteClusters.put(descriptor, descriptor)
            }
        }
    }

    private fun hasDiverged(
        targetClusterConfig: ClusterConfig,
        deployments: MutableMap<ClusterDescriptor, V1Deployment>,
        jobmanagerStatefulSets: MutableMap<ClusterDescriptor, V1StatefulSet>,
        taskmanagerStatefulSets: MutableMap<ClusterDescriptor, V1StatefulSet>,
        services: MutableMap<ClusterDescriptor, V1Service>,
        jobmanagerPersistentVolumeClaims: MutableMap<ClusterDescriptor, V1PersistentVolumeClaim>,
        taskmanagerPersistentVolumeClaims: MutableMap<ClusterDescriptor, V1PersistentVolumeClaim>
    ) : Boolean {
        val service = services.get(targetClusterConfig.descriptor)
        val deployment = deployments.get(targetClusterConfig.descriptor)
        val jobmanagerStatefulSet = jobmanagerStatefulSets.get(targetClusterConfig.descriptor)
        val taskmanagerStatefulSet = taskmanagerStatefulSets.get(targetClusterConfig.descriptor)
        val jobmanagerPersistentVolumeClaim = jobmanagerPersistentVolumeClaims.get(targetClusterConfig.descriptor)
        val taskmanagerPersistentVolumeClaim = taskmanagerPersistentVolumeClaims.get(targetClusterConfig.descriptor)

        if (service == null) {
            return true
        }

        if (deployment == null) {
            return true
        }

        if (jobmanagerStatefulSet == null) {
            return true
        }

        if (taskmanagerStatefulSet == null) {
            return true
        }

        if (jobmanagerPersistentVolumeClaim == null) {
            return true
        }

        if (taskmanagerPersistentVolumeClaim == null) {
            return true
        }

        if (deployment.spec.template.spec.containers.size != 1) {
            return true
        }

        if (deployment.metadata.labels.get("cluster") == null) {
            return true
        }

        if (deployment.metadata.labels.get("component") == null) {
            return true
        }

        if (deployment.metadata.labels.get("environment") == null) {
            return true
        }

        if (service.metadata.labels.get("cluster") == null) {
            return true
        }

        if (service.metadata.labels.get("role") == null) {
            return true
        }

        if (service.metadata.labels.get("component") == null) {
            return true
        }

        if (service.metadata.labels.get("environment") == null) {
            return true
        }

        if (jobmanagerStatefulSet.metadata.labels.get("cluster") == null) {
            return true
        }

        if (jobmanagerStatefulSet.metadata.labels.get("role") == null) {
            return true
        }

        if (jobmanagerStatefulSet.metadata.labels.get("component") == null) {
            return true
        }

        if (jobmanagerStatefulSet.metadata.labels.get("environment") == null) {
            return true
        }

        if (taskmanagerStatefulSet.metadata.labels.get("cluster") == null) {
            return true
        }

        if (taskmanagerStatefulSet.metadata.labels.get("role") == null) {
            return true
        }

        if (taskmanagerStatefulSet.metadata.labels.get("component") == null) {
            return true
        }

        if (taskmanagerStatefulSet.metadata.labels.get("environment") == null) {
            return true
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels.get("cluster") == null) {
            return true
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels.get("role") == null) {
            return true
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels.get("component") == null) {
            return true
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels.get("environment") == null) {
            return true
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels.get("cluster") == null) {
            return true
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels.get("role") == null) {
            return true
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels.get("component") == null) {
            return true
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels.get("environment") == null) {
            return true
        }

        val sidecarImage = deployment.spec.template.spec.containers.get(0).image
        val sidecarPullPolicy = deployment.spec.template.spec.containers.get(0).imagePullPolicy
        val sidecarServiceAccount = deployment.spec.template.spec.serviceAccount

        val containerArguments = deployment.spec.template.spec.containers.get(0).args

        if (containerArguments.get(0) != "sidecar") {
            logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        if (containerArguments.get(1) != "submit" && containerArguments.get(1) != "watch") {
            logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        val sidecarNamespace = containerArguments.filter{ it.startsWith("--namespace") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarEnvironment = containerArguments.filter{ it.startsWith("--environment") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarClusterName = containerArguments.filter{ it.startsWith("--cluster-name") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarJarPath = containerArguments.filter{ it.startsWith("--jar-path") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarClassName = containerArguments.filter{ it.startsWith("--class-name") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarSavepoint = containerArguments.filter{ it.startsWith("--savepoint") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarParallelism = containerArguments.filter{ it.startsWith("--parallelism") }.map { it.substringAfter("=") }.firstOrNull()

        if (sidecarNamespace == null || sidecarNamespace != targetClusterConfig.descriptor.namespace) {
            logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        if (sidecarEnvironment == null || sidecarEnvironment != targetClusterConfig.descriptor.environment) {
            logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        if (sidecarClusterName == null || sidecarClassName != targetClusterConfig.descriptor.name) {
            logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        if (containerArguments.get(1) == "submit" && sidecarJarPath == null) {
            logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        val sidecarArguments = containerArguments.filter{ it.startsWith("--argument") }.map { it.substringAfter("=") }.toList()

        val pullSecrets = if (deployment.spec.template.spec.imagePullSecrets != null && !deployment.spec.template.spec.imagePullSecrets.isEmpty()) deployment.spec.template.spec.imagePullSecrets.get(0).name else null

        if (jobmanagerStatefulSet.spec.template.spec.containers.size != 1) {
            return true
        }

        val jobmanagerImage = jobmanagerStatefulSet.spec.template.spec.containers.get(0).image
        val jobmanagerPullPolicy = jobmanagerStatefulSet.spec.template.spec.containers.get(0).imagePullPolicy
        val jobmanagerServiceAccount = jobmanagerStatefulSet.spec.template.spec.serviceAccount

        val jobmanagerCpuQuantity = jobmanagerStatefulSet.spec.template.spec.containers.get(0).resources.limits.get("cpu")

        if (jobmanagerCpuQuantity == null) {
            return true
        }

        val jobmanagerCpu = jobmanagerCpuQuantity.number.toFloat()

        if (jobmanagerStatefulSet.spec.volumeClaimTemplates.size != 1) {
            return true
        }

        val jobmanagerStorageClassName = jobmanagerStatefulSet.spec.volumeClaimTemplates.get(0).spec.storageClassName
        val jobmanagerStorageSizeQuantity = jobmanagerStatefulSet.spec.volumeClaimTemplates.get(0).spec.resources.requests.get("storage")

        if (jobmanagerStorageSizeQuantity == null) {
            return true
        }

        val jobmanagerStorageSize = jobmanagerStorageSizeQuantity.number.toInt()

        if (taskmanagerStatefulSet.spec.template.spec.containers.size != 1) {
            return true
        }

        val taskmanagerImage = taskmanagerStatefulSet.spec.template.spec.containers.get(0).image
        val taskmanagerPullPolicy = taskmanagerStatefulSet.spec.template.spec.containers.get(0).imagePullPolicy
        val taskmanagerServiceAccount = taskmanagerStatefulSet.spec.template.spec.serviceAccount

        val taskmanagerCpuQuantity = taskmanagerStatefulSet.spec.template.spec.containers.get(0).resources.limits.get("cpu")

        if (taskmanagerCpuQuantity == null) {
            return true
        }

        val taskmanagerCpu = taskmanagerCpuQuantity.number.toFloat()

        if (taskmanagerStatefulSet.spec.volumeClaimTemplates.size != 1) {
            return true
        }

        val taskmanagerStorageClassName = taskmanagerStatefulSet.spec.volumeClaimTemplates.get(0).spec.storageClassName
        val taskmanagerStorageSizeQuantity = taskmanagerStatefulSet.spec.volumeClaimTemplates.get(0).spec.resources.requests.get("storage")

        if (taskmanagerStorageSizeQuantity == null) {
            return true
        }

        val taskmanagerStorageSize = taskmanagerStorageSizeQuantity.number.toInt()

        val taskmanagerReplicas = taskmanagerStatefulSet.spec.replicas

        val environment = jobmanagerStatefulSet.metadata.labels.get("environment").orEmpty()

        val jobmanagerMemory = jobmanagerStatefulSet.spec.template.spec.containers.get(0).env.get(4).value.toInt()
        val taskmanagerMemory = taskmanagerStatefulSet.spec.template.spec.containers.get(0).env.get(4).value.toInt()
        val taskmanagerTaskSlots = taskmanagerStatefulSet.spec.template.spec.containers.get(0).env.get(5).value.toInt()

        val serviceMode = service.spec.type

        val clusterConfig = ClusterConfig(
            descriptor = ClusterDescriptor(
                namespace = targetClusterConfig.descriptor.namespace,
                name = targetClusterConfig.descriptor.name,
                environment = environment
            ),
            jobmanager = JobManagerConfig(
                image = jobmanagerImage,
                pullSecrets = pullSecrets,
                pullPolicy = jobmanagerPullPolicy,
                serviceMode = serviceMode,
                serviceAccount = jobmanagerServiceAccount,
                resources = ResourcesConfig(
                    cpus = jobmanagerCpu,
                    memory = jobmanagerMemory
                ),
                storage = StorageConfig(
                    storageClass = jobmanagerStorageClassName,
                    size = jobmanagerStorageSize
                )
            ),
            taskmanager = TaskManagerConfig(
                image = taskmanagerImage,
                pullSecrets = pullSecrets,
                pullPolicy = taskmanagerPullPolicy,
                serviceAccount = taskmanagerServiceAccount,
                replicas = taskmanagerReplicas,
                taskSlots = taskmanagerTaskSlots,
                resources = ResourcesConfig(
                    cpus = taskmanagerCpu,
                    memory = taskmanagerMemory
                ),
                storage = StorageConfig(
                    storageClass = taskmanagerStorageClassName,
                    size = taskmanagerStorageSize
                )
            ),
            sidecar = SidecarConfig(
                image = sidecarImage,
                pullSecrets = pullSecrets,
                pullPolicy = sidecarPullPolicy,
                serviceAccount = sidecarServiceAccount,
                className = sidecarClassName,
                jarPath = sidecarJarPath,
                savepoint = sidecarSavepoint,
                arguments = sidecarArguments.joinToString(" "),
                parallelism = sidecarParallelism?.toInt() ?: 1
            )
        )

        val diverged = clusterConfig.equals(targetClusterConfig).not()

        if (diverged) {
            logger.info("Current config: $clusterConfig")
            logger.info("Desired config: $targetClusterConfig")
        }

        return diverged
    }

    private fun createClusterConfig(
        metadata: V1ObjectMeta,
        spec: V1FlinkClusterSpec
    ): ClusterConfig {
        val clusterConfig = ClusterConfig(
            descriptor = ClusterDescriptor(
                namespace = metadata.namespace,
                name = metadata.name,
                environment = spec.environment ?: "test"
            ),
            jobmanager = JobManagerConfig(
                image = spec.flinkImage,
                pullSecrets = spec.pullSecrets,
                pullPolicy = spec.pullPolicy ?: "Always",
                serviceMode = spec.serviceMode ?: "NodePort",
                serviceAccount = spec.jobmanagerServiceAccount ?: "default",
                resources = ResourcesConfig(
                    cpus = spec.jobmanagerCpus ?: 1f,
                    memory = spec.jobmanagerMemory ?: 512
                ),
                storage = StorageConfig(
                    storageClass = spec.jobmanagerStorageClass ?: "standard",
                    size = spec.jobmanagerStorageSize ?: 2
                )
            ),
            taskmanager = TaskManagerConfig(
                image = spec.flinkImage,
                pullSecrets = spec.pullSecrets,
                pullPolicy = spec.pullPolicy ?: "Always",
                serviceAccount = spec.taskmanagerServiceAccount ?: "default",
                replicas = spec.taskmanagerReplicas ?: 1,
                taskSlots = spec.taskmanagerTaskSlots ?: 1,
                resources = ResourcesConfig(
                    cpus = spec.taskmanagerCpus ?: 1f,
                    memory = spec.taskmanagerMemory ?: 1024
                ),
                storage = StorageConfig(
                    storageClass = spec.taskmanagerStorageClass ?: "standard",
                    size = spec.taskmanagerStorageSize ?: 2
                )
            ),
            sidecar = SidecarConfig(
                image = spec.sidecarImage,
                pullSecrets = spec.pullSecrets,
                pullPolicy = spec.pullPolicy ?: "Always",
                serviceAccount = spec.sidecarServiceAccount ?: "default",
                className = spec.sidecarClassName,
                jarPath = spec.sidecarJarPath,
                savepoint = spec.sidecarSavepoint,
                arguments = spec.sidecarArguments?.joinToString(" "),
                parallelism = spec.sidecarParallelism ?: 1
            )
        )
        return clusterConfig
    }

    private fun watchFlickClusterResources(namespace: String, objectApi: CustomObjectsApi): Watch<V1FlinkCluster> =
        Watch.createWatch<V1FlinkCluster>(
            Configuration.getDefaultApiClient(),
            objectApi.listNamespacedCustomObjectCall(
                "beta.nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
                null,
                null,
                null,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1FlinkCluster>>() {}.type
        )

    private fun watchServiceResources(namespace: String, coreApi: CoreV1Api): Watch<V1Service> =
        Watch.createWatch<V1Service>(
            Configuration.getDefaultApiClient(),
            coreApi.listNamespacedServiceCall(
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
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1Service>>() {}.type
        )

    private fun watchDeploymentResources(namespace: String, appsApi: AppsV1Api): Watch<V1Deployment> =
        Watch.createWatch<V1Deployment>(
            Configuration.getDefaultApiClient(),
            appsApi.listNamespacedDeploymentCall(
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
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1Deployment>>() {}.type
        )

    private fun watchStatefulSetResources(namespace: String, appsApi: AppsV1Api): Watch<V1StatefulSet> =
        Watch.createWatch<V1StatefulSet>(
            Configuration.getDefaultApiClient(),
            appsApi.listNamespacedStatefulSetCall(
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
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1StatefulSet>>() {}.type
        )

    private fun watchPermanentVolumeClaimResources(namespace: String, coreApi: CoreV1Api): Watch<V1PersistentVolumeClaim> =
        Watch.createWatch<V1PersistentVolumeClaim>(
            Configuration.getDefaultApiClient(),
            coreApi.listNamespacedPersistentVolumeClaimCall(
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
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1PersistentVolumeClaim>>() {}.type
        )
}
