package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class DefaultClusterResourcesFactoryTest {
    private val cluster = TestFactory.aCluster(name = "test", namespace ="flink", taskManagers = 3, taskSlots = 2)

    @Test
    fun `should create job manager service`() {
        val service = DefaultClusterResourcesFactory.createJobManagerService("test", "xxx", "myself", cluster)

        assertThat(service).isNotNull()

        assertThat(service?.metadata?.name).isEqualTo("flink-jobmanager-${cluster.metadata.name}")

        val labels = service?.metadata?.labels
        assertThat(labels).hasSize(5)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("name")).isEqualTo(cluster.metadata.name)
        assertThat(labels?.get("uid")).isEqualTo("xxx")
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("jobmanager")

        assertThat(service?.spec?.type).isEqualTo("ClusterIP")

        val ports = service?.spec?.ports
        assertThat(ports).hasSize(4)
        assertThat(ports?.get(0)?.name).isEqualTo("ui")
        assertThat(ports?.get(1)?.name).isEqualTo("rpc")
        assertThat(ports?.get(2)?.name).isEqualTo("blob")
        assertThat(ports?.get(3)?.name).isEqualTo("query")

        val selector = service?.spec?.selector
        assertThat(selector).hasSize(5)
        assertThat(selector?.get("owner")).isNotNull()
        assertThat(selector?.get("name")).isNotNull()
        assertThat(selector?.get("uid")).isEqualTo("xxx")
        assertThat(selector?.get("component")).isNotNull()
        assertThat(selector?.get("role")).isNotNull()
    }

    @Test
    fun `should create bootstrap job when submitting job`() {
        val job = DefaultClusterResourcesFactory.createBootstrapJob("test", "xxx", "myself", cluster)

        assertThat(job).isNotNull()

        assertThat(job?.metadata?.name).isEqualTo("flink-bootstrap-${cluster.metadata.name}")

        val labels = job?.metadata?.labels
        assertThat(labels).hasSize(4)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("name")).isEqualTo(cluster.metadata.name)
        assertThat(labels?.get("uid")).isEqualTo("xxx")
        assertThat(labels?.get("component")).isEqualTo("flink")

        assertThat(job?.spec?.parallelism).isEqualTo(1)
        assertThat(job?.spec?.completions).isEqualTo(1)
        assertThat(job?.spec?.backoffLimit).isEqualTo(3)
        assertThat(job?.spec?.ttlSecondsAfterFinished).isEqualTo(30)

        val podSpec = job?.spec?.template?.spec
        assertThat(podSpec?.restartPolicy).isEqualTo("OnFailure")
        assertThat(podSpec?.serviceAccountName).isEqualTo("bootstrap-test")
        assertThat(podSpec?.imagePullSecrets).hasSize(1)
        assertThat(podSpec?.imagePullSecrets?.get(0)?.name).isEqualTo("bootstrap-regcred")
        assertThat(podSpec?.affinity).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity?.preferredDuringSchedulingIgnoredDuringExecution).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity?.preferredDuringSchedulingIgnoredDuringExecution).hasSize(1)
        assertThat(podSpec?.containers).hasSize(1)

        val container = podSpec?.containers?.get(0)
        assertThat(container?.ports).isNull()
        assertThat(container?.imagePullPolicy).isEqualTo("IfNotPresent")
        assertThat(container?.args).hasSize(5)
        assertThat(container?.args?.get(0)).isEqualTo("bootstrap")
        assertThat(container?.args?.get(1)).isEqualTo("upload")
        assertThat(container?.args?.get(2)).isEqualTo("--namespace=test")
        assertThat(container?.args?.get(3)).isEqualTo("--cluster-name=${cluster.metadata.name}")
        assertThat(container?.args?.get(4)).isEqualTo("--jar-path=${cluster.spec?.bootstrap?.jarPath}")
        assertThat(container?.env).hasSize(2)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
    }

    @Test
    fun `should create job manager statefulset`() {
        val statefulset = DefaultClusterResourcesFactory.createJobManagerStatefulSet("test", "xxx", "myself", cluster)

        assertThat(statefulset).isNotNull()

        assertThat(statefulset?.metadata?.name).isEqualTo("flink-jobmanager-${cluster.metadata.name}")

        val labels = statefulset?.metadata?.labels
        assertThat(labels).hasSize(5)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("name")).isEqualTo(cluster.metadata.name)
        assertThat(labels?.get("uid")).isEqualTo("xxx")
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("jobmanager")

        assertThat(statefulset?.spec?.replicas).isEqualTo(1)
        assertThat(statefulset?.spec?.updateStrategy).isNotNull()
        assertThat(statefulset?.spec?.serviceName).isEqualTo("jobmanager")
        assertThat(statefulset?.spec?.selector).isNotNull()

        val matchLabels = statefulset?.spec?.selector?.matchLabels
        assertThat(matchLabels).hasSize(5)
        assertThat(matchLabels?.get("owner")).isNotNull()
        assertThat(matchLabels?.get("name")).isNotNull()
        assertThat(matchLabels?.get("uid")).isEqualTo("xxx")
        assertThat(matchLabels?.get("component")).isNotNull()
        assertThat(matchLabels?.get("role")).isNotNull()

        assertThat(statefulset?.spec?.volumeClaimTemplates).hasSize(1)
        assertThat(statefulset?.spec?.volumeClaimTemplates?.get(0)?.spec?.storageClassName).isEqualTo("hostpath")
        assertThat(statefulset?.spec?.volumeClaimTemplates?.get(0)?.spec?.resources?.requests?.get("storage")?.toSuffixedString()).isEqualTo("1Gi")

        val podSpec = statefulset?.spec?.template?.spec
        assertThat(podSpec?.serviceAccountName).isEqualTo("jobmanager-test")
        assertThat(podSpec?.imagePullSecrets).hasSize(1)
        assertThat(podSpec?.imagePullSecrets?.get(0)?.name).isEqualTo("flink-regcred")
        assertThat(podSpec?.affinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).hasSize(2)

        assertThat(podSpec?.containers).hasSize(2)
        assertThat(podSpec?.initContainers).hasSize(1)

        val container = podSpec?.containers?.get(0)
        assertThat(container?.image).isEqualTo("registry:30000/flink:1.9.0")
        assertThat(container?.imagePullPolicy).isEqualTo("IfNotPresent")
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
        assertThat(container?.volumeMounts).hasSize(4)
        assertThat(container?.volumeMounts?.get(3)?.name).isEqualTo("jobmanager")
        assertThat(container?.resources).isEqualTo(cluster.spec.jobManager.resources)

        assertThat(container?.env?.get(2)?.value).isEqualTo("512")
    }

    @Test
    fun `should create task manager statefulset`() {
        val statefulset = DefaultClusterResourcesFactory.createTaskManagerStatefulSet("test", "xxx", "myself", cluster)

        assertThat(statefulset).isNotNull()

        assertThat(statefulset?.metadata?.name).isEqualTo("flink-taskmanager-${cluster.metadata.name}")

        val labels = statefulset?.metadata?.labels
        assertThat(labels).hasSize(5)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("name")).isEqualTo(cluster.metadata.name)
        assertThat(labels?.get("uid")).isEqualTo("xxx")
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("taskmanager")

        assertThat(statefulset?.spec?.replicas).isEqualTo(3)
        assertThat(statefulset?.spec?.updateStrategy).isNotNull()
        assertThat(statefulset?.spec?.serviceName).isEqualTo("taskmanager")
        assertThat(statefulset?.spec?.selector).isNotNull()

        val matchLabels = statefulset?.spec?.selector?.matchLabels
        assertThat(matchLabels).hasSize(5)
        assertThat(matchLabels?.get("owner")).isNotNull()
        assertThat(matchLabels?.get("name")).isNotNull()
        assertThat(matchLabels?.get("uid")).isNotNull()
        assertThat(matchLabels?.get("component")).isNotNull()
        assertThat(matchLabels?.get("role")).isNotNull()

        assertThat(statefulset?.spec?.volumeClaimTemplates).hasSize(1)
        assertThat(statefulset?.spec?.volumeClaimTemplates?.get(0)?.spec?.storageClassName).isEqualTo("hostpath")
        assertThat(statefulset?.spec?.volumeClaimTemplates?.get(0)?.spec?.resources?.requests?.get("storage")?.toSuffixedString()).isEqualTo("5Gi")

        val podSpec = statefulset?.spec?.template?.spec
        assertThat(podSpec?.serviceAccountName).isEqualTo("taskmanager-test")
        assertThat(podSpec?.imagePullSecrets).hasSize(1)
        assertThat(podSpec?.imagePullSecrets?.get(0)?.name).isEqualTo("flink-regcred")
        assertThat(podSpec?.affinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).hasSize(2)

        assertThat(podSpec?.containers).hasSize(2)
        assertThat(podSpec?.initContainers).hasSize(1)

        val container = podSpec?.containers?.get(0)
        assertThat(container?.image).isEqualTo("registry:30000/flink:1.9.0")
        assertThat(container?.imagePullPolicy).isEqualTo("IfNotPresent")
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
        assertThat(container?.env?.get(4)?.value).isEqualTo("2")
        assertThat(container?.env?.get(5)?.name).isEqualTo("FLINK_GRAPHITE_HOST")
        assertThat(container?.env?.get(5)?.value).isEqualTo("graphite.default.svc.cluster.local")
        assertThat(container?.volumeMounts).hasSize(4)
        assertThat(container?.volumeMounts?.get(3)?.name).isEqualTo("taskmanager")
        assertThat(container?.resources).isEqualTo(cluster.spec.taskManager.resources)

        assertThat(container?.env?.get(2)?.value).isEqualTo("2048")
    }
}