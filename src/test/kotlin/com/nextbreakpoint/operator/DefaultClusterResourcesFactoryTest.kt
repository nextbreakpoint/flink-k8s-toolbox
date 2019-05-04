package com.nextbreakpoint.operator

import com.nextbreakpoint.common.TestFactory
import com.nextbreakpoint.operator.resources.DefaultClusterResourcesFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

@RunWith(JUnitPlatform::class)
class DefaultClusterResourcesFactoryTest {
    private val cluster = TestFactory.aCluster()

    @Test
    fun `should create jobmanager service`() {
        val service = DefaultClusterResourcesFactory.createJobManagerService("test", "xxx", "myself", cluster)

        assertThat(service.metadata?.name).isEqualTo("flink-jobmanager-${cluster.metadata.name}")

        val labels = service.metadata?.labels
        assertThat(labels).hasSize(5)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("name")).isEqualTo(cluster.metadata.name)
        assertThat(labels?.get("uid")).isEqualTo("xxx")
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("jobmanager")

        assertThat(service.spec?.type).isEqualTo("ClusterIP")

        val ports = service.spec?.ports
        assertThat(ports).hasSize(4)
        assertThat(ports?.get(0)?.name).isEqualTo("ui")
        assertThat(ports?.get(1)?.name).isEqualTo("rpc")
        assertThat(ports?.get(2)?.name).isEqualTo("blob")
        assertThat(ports?.get(3)?.name).isEqualTo("query")

        val selector = service.spec?.selector
        assertThat(selector).hasSize(5)
        assertThat(selector?.get("owner")).isNotNull()
        assertThat(selector?.get("name")).isNotNull()
        assertThat(selector?.get("uid")).isEqualTo("xxx")
        assertThat(selector?.get("component")).isNotNull()
        assertThat(selector?.get("role")).isNotNull()
    }

    @Test
    fun `should create jar upload job when submitting job`() {
        val job = DefaultClusterResourcesFactory.createJarUploadJob("test", "xxx", "myself", cluster)

        assertThat(job.metadata?.name).isEqualTo("flink-upload-${cluster.metadata.name}")

        val labels = job.metadata?.labels
        assertThat(labels).hasSize(4)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("name")).isEqualTo(cluster.metadata.name)
        assertThat(labels?.get("uid")).isEqualTo("xxx")
        assertThat(labels?.get("component")).isEqualTo("flink")

        assertThat(job.spec?.parallelism).isEqualTo(1)
        assertThat(job.spec?.completions).isEqualTo(1)
        assertThat(job.spec?.backoffLimit).isEqualTo(3)
        assertThat(job.spec?.ttlSecondsAfterFinished).isEqualTo(30)

        val podSpec = job.spec?.template?.spec
        assertThat(podSpec?.restartPolicy).isEqualTo("OnFailure")
        assertThat(podSpec?.serviceAccountName).isEqualTo("flink-upload")
        assertThat(podSpec?.imagePullSecrets).hasSize(1)
        assertThat(podSpec?.imagePullSecrets?.get(0)?.name).isEqualTo(cluster.spec.pullSecrets)
        assertThat(podSpec?.affinity).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity?.preferredDuringSchedulingIgnoredDuringExecution).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity?.preferredDuringSchedulingIgnoredDuringExecution).hasSize(1)
        assertThat(podSpec?.containers).hasSize(1)

        val container = podSpec?.containers?.get(0)
        assertThat(container?.ports).isNull()
        assertThat(container?.imagePullPolicy).isEqualTo(cluster.spec.pullPolicy ?: "Always")
        assertThat(container?.args).hasSize(5)
        assertThat(container?.args?.get(0)).isEqualTo("upload")
        assertThat(container?.args?.get(1)).isEqualTo("jar")
        assertThat(container?.args?.get(2)).isEqualTo("--namespace=test")
        assertThat(container?.args?.get(3)).isEqualTo("--cluster-name=${cluster.metadata.name}")
        assertThat(container?.args?.get(4)).isEqualTo("--jar-path=${cluster.spec.jobJarPath}")
        assertThat(container?.env).hasSize(2)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
    }

    @Test
    fun `should create jobmanager statefulset`() {
        val statefulset = DefaultClusterResourcesFactory.createJobManagerStatefulSet("test", "xxx", "myself", cluster)

        assertThat(statefulset.metadata?.name).isEqualTo("flink-jobmanager-${cluster.metadata.name}")

        val labels = statefulset.metadata?.labels
        assertThat(labels).hasSize(5)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("name")).isEqualTo(cluster.metadata.name)
        assertThat(labels?.get("uid")).isEqualTo("xxx")
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("jobmanager")

        assertThat(statefulset.spec?.replicas).isEqualTo(1)
        assertThat(statefulset.spec?.updateStrategy).isNotNull()
        assertThat(statefulset.spec?.serviceName).isEqualTo("jobmanager")
        assertThat(statefulset.spec?.selector).isNotNull()

        val matchLabels = statefulset.spec?.selector?.matchLabels
        assertThat(matchLabels).hasSize(5)
        assertThat(matchLabels?.get("owner")).isNotNull()
        assertThat(matchLabels?.get("name")).isNotNull()
        assertThat(matchLabels?.get("uid")).isEqualTo("xxx")
        assertThat(matchLabels?.get("component")).isNotNull()
        assertThat(matchLabels?.get("role")).isNotNull()

        assertThat(statefulset.spec?.volumeClaimTemplates).hasSize(1)
        assertThat(statefulset.spec?.volumeClaimTemplates?.get(0)?.spec?.storageClassName).isEqualTo(cluster.spec.jobmanagerStorageClass ?: "standard")
        assertThat(statefulset.spec?.volumeClaimTemplates?.get(0)?.spec?.resources?.requests?.get("storage")?.number?.toInt()).isEqualTo(cluster.spec.jobmanagerStorageSize ?: 1)

        val podSpec = statefulset.spec?.template?.spec
        assertThat(podSpec?.serviceAccountName).isEqualTo(cluster.spec.serviceAccount ?: "default")
        assertThat(podSpec?.imagePullSecrets).hasSize(1)
        assertThat(podSpec?.imagePullSecrets?.get(0)?.name).isEqualTo(cluster.spec.pullSecrets)
        assertThat(podSpec?.affinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).hasSize(2)
        assertThat(podSpec?.containers).hasSize(1)

        val container = podSpec?.containers?.get(0)
        assertThat(container?.image).isEqualTo(cluster.spec.flinkImage)
        assertThat(container?.imagePullPolicy).isEqualTo(cluster.spec.pullPolicy ?: "Always")
        assertThat(container?.ports).hasSize(4)
        assertThat(container?.ports?.get(0)?.name).isEqualTo("ui")
        assertThat(container?.ports?.get(1)?.name).isEqualTo("rpc")
        assertThat(container?.ports?.get(2)?.name).isEqualTo("blob")
        assertThat(container?.ports?.get(3)?.name).isEqualTo("query")
        assertThat(container?.args).hasSize(1)
        assertThat(container?.args?.get(0)).isEqualTo("jobmanager")
        assertThat(container?.env).hasSize(5)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
        assertThat(container?.env?.get(2)?.name).isEqualTo("FLINK_JM_HEAP")
        assertThat(container?.env?.get(3)?.name).isEqualTo("JOB_MANAGER_RPC_ADDRESS")
        assertThat(container?.env?.get(4)?.name).isEqualTo("FLINK_GRAPHITE_HOST")
        assertThat(container?.env?.get(4)?.value).isEqualTo("graphite.default.svc.cluster.local")
        assertThat(container?.volumeMounts).hasSize(1)
        assertThat(container?.volumeMounts?.get(0)?.name).isEqualTo("jobmanager")
        assertThat(container?.resources?.limits?.get("cpu")?.number?.toFloat()).isEqualTo(cluster.spec.jobmanagerCPUs ?: 1.0f)
        assertThat(container?.resources?.requests?.get("memory")?.number?.toInt()).isEqualTo((cluster.spec.jobmanagerMemory ?: 256) * 1024 * 1024)
    }

    @Test
    fun `should create taskmanager statefulset`() {
        val statefulset = DefaultClusterResourcesFactory.createTaskManagerStatefulSet("test", "xxx", "myself", cluster)

        assertThat(statefulset.metadata?.name).isEqualTo("flink-taskmanager-${cluster.metadata.name}")

        val labels = statefulset.metadata?.labels
        assertThat(labels).hasSize(5)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("name")).isEqualTo(cluster.metadata.name)
        assertThat(labels?.get("uid")).isEqualTo("xxx")
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("taskmanager")

        assertThat(statefulset.spec?.replicas).isEqualTo(cluster.spec.taskmanagerReplicas ?: 1)
        assertThat(statefulset.spec?.updateStrategy).isNotNull()
        assertThat(statefulset.spec?.serviceName).isEqualTo("taskmanager")
        assertThat(statefulset.spec?.selector).isNotNull()

        val matchLabels = statefulset.spec?.selector?.matchLabels
        assertThat(matchLabels).hasSize(5)
        assertThat(matchLabels?.get("owner")).isNotNull()
        assertThat(matchLabels?.get("name")).isNotNull()
        assertThat(matchLabels?.get("uid")).isNotNull()
        assertThat(matchLabels?.get("component")).isNotNull()
        assertThat(matchLabels?.get("role")).isNotNull()

        assertThat(statefulset.spec?.volumeClaimTemplates).hasSize(1)
        assertThat(statefulset.spec?.volumeClaimTemplates?.get(0)?.spec?.storageClassName).isEqualTo(cluster.spec.taskmanagerStorageClass ?: "standard")
        assertThat(statefulset.spec?.volumeClaimTemplates?.get(0)?.spec?.resources?.requests?.get("storage")?.number?.toInt()).isEqualTo(cluster.spec.taskmanagerStorageSize ?: 5)

        val podSpec = statefulset.spec?.template?.spec
        assertThat(podSpec?.serviceAccountName).isEqualTo(cluster.spec.serviceAccount ?: "default")
        assertThat(podSpec?.imagePullSecrets).hasSize(1)
        assertThat(podSpec?.imagePullSecrets?.get(0)?.name).isEqualTo(cluster.spec.pullSecrets)
        assertThat(podSpec?.affinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).hasSize(2)
        assertThat(podSpec?.containers).hasSize(1)

        val container = podSpec?.containers?.get(0)
        assertThat(container?.image).isEqualTo(cluster.spec.flinkImage)
        assertThat(container?.imagePullPolicy).isEqualTo(cluster.spec.pullPolicy ?: "Always")
        assertThat(container?.ports).hasSize(2)
        assertThat(container?.ports?.get(0)?.name).isEqualTo("data")
        assertThat(container?.ports?.get(1)?.name).isEqualTo("ipc")
        assertThat(container?.args).hasSize(1)
        assertThat(container?.args?.get(0)).isEqualTo("taskmanager")
        assertThat(container?.env).hasSize(6)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
        assertThat(container?.env?.get(2)?.name).isEqualTo("FLINK_TM_HEAP")
        assertThat(container?.env?.get(3)?.name).isEqualTo("JOB_MANAGER_RPC_ADDRESS")
        assertThat(container?.env?.get(4)?.name).isEqualTo("TASK_MANAGER_NUMBER_OF_TASK_SLOTS")
        assertThat(container?.env?.get(4)?.value).isEqualTo("${cluster.spec.taskmanagerTaskSlots ?: 1}")
        assertThat(container?.env?.get(5)?.name).isEqualTo("FLINK_GRAPHITE_HOST")
        assertThat(container?.env?.get(5)?.value).isEqualTo("graphite.default.svc.cluster.local")
        assertThat(container?.volumeMounts).hasSize(1)
        assertThat(container?.volumeMounts?.get(0)?.name).isEqualTo("taskmanager")
        assertThat(container?.resources?.limits?.get("cpu")?.number?.toFloat()).isEqualTo(cluster.spec.taskmanagerCPUs ?: 1.0f)
        assertThat(container?.resources?.requests?.get("memory")?.number?.toInt()).isEqualTo((cluster.spec.taskmanagerMemory ?: 1024) * 1024 * 1024)
    }
}