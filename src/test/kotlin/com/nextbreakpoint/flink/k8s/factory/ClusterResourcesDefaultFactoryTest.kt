package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ClusterResourcesDefaultFactoryTest {
    private val cluster = TestFactory.aFlinkCluster(name = "test", namespace ="flink", taskManagers = 3, taskSlots = 2)

    @Test
    fun `should create jobmanager service`() {
        val service = ClusterResourcesDefaultFactory.createService("test", "myself", "test", cluster.spec)

        assertThat(service).isNotNull()

        assertThat(service.metadata?.name).isEqualTo("jobmanager-${cluster.metadata.name}")

        val labels = service.metadata?.labels
        assertThat(labels).hasSize(4)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("clusterName")).isEqualTo(cluster.metadata.name)
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
        assertThat(selector).hasSize(4)
        assertThat(selector?.get("owner")).isNotNull()
        assertThat(selector?.get("clusterName")).isNotNull()
        assertThat(selector?.get("component")).isNotNull()
        assertThat(selector?.get("role")).isNotNull()
    }

    @Test
    fun `should create jobmanager pod`() {
        val pod = ClusterResourcesDefaultFactory.createJobManagerPod("test", "myself", "test", cluster.spec)

        assertThat(pod).isNotNull()

        assertThat(pod.metadata?.generateName).isEqualTo("jobmanager-${cluster.metadata.name}-")

        val labels = pod.metadata?.labels
        assertThat(labels).hasSize(4)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("clusterName")).isEqualTo(cluster.metadata.name)
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("jobmanager")

        val podSpec = pod.spec
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
        assertThat(container?.image).isEqualTo("registry:30000/flink:1.9.2")
        assertThat(container?.imagePullPolicy).isEqualTo("IfNotPresent")
        assertThat(container?.ports).hasSize(4)
        assertThat(container?.ports?.get(0)?.name).isEqualTo("ui")
        assertThat(container?.ports?.get(1)?.name).isEqualTo("rpc")
        assertThat(container?.ports?.get(2)?.name).isEqualTo("blob")
        assertThat(container?.ports?.get(3)?.name).isEqualTo("query")
        assertThat(container?.args).hasSize(1)
        assertThat(container?.args?.get(0)).isEqualTo("jobmanager")
        assertThat(container?.env).hasSize(4)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
        assertThat(container?.env?.get(2)?.name).isEqualTo("JOB_MANAGER_RPC_ADDRESS")
        assertThat(container?.env?.get(3)?.name).isEqualTo("FLINK_GRAPHITE_HOST")
        assertThat(container?.env?.get(3)?.value).isEqualTo("graphite.default.svc.cluster.local")
        assertThat(container?.volumeMounts).hasSize(4)
        assertThat(container?.volumeMounts?.get(3)?.name).isEqualTo("jobmanager")
        assertThat(container?.resources).isEqualTo(cluster.spec.jobManager.resources)
    }

    @Test
    fun `should create taskmanager pod`() {
        val pod = ClusterResourcesDefaultFactory.createTaskManagerPod("test", "myself", "test", cluster.spec)

        assertThat(pod).isNotNull()

        assertThat(pod.metadata?.generateName).isEqualTo("taskmanager-${cluster.metadata.name}-")

        val labels = pod.metadata?.labels
        assertThat(labels).hasSize(4)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("clusterName")).isEqualTo(cluster.metadata.name)
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("taskmanager")

        val podSpec = pod.spec
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
        assertThat(container?.image).isEqualTo("registry:30000/flink:1.9.2")
        assertThat(container?.imagePullPolicy).isEqualTo("IfNotPresent")
        assertThat(container?.ports).hasSize(2)
        assertThat(container?.ports?.get(0)?.name).isEqualTo("data")
        assertThat(container?.ports?.get(1)?.name).isEqualTo("ipc")
        assertThat(container?.args).hasSize(1)
        assertThat(container?.args?.get(0)).isEqualTo("taskmanager")
        assertThat(container?.env).hasSize(5)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
        assertThat(container?.env?.get(2)?.name).isEqualTo("JOB_MANAGER_RPC_ADDRESS")
        assertThat(container?.env?.get(3)?.name).isEqualTo("TASK_MANAGER_NUMBER_OF_TASK_SLOTS")
        assertThat(container?.env?.get(3)?.value).isEqualTo("2")
        assertThat(container?.env?.get(4)?.name).isEqualTo("FLINK_GRAPHITE_HOST")
        assertThat(container?.env?.get(4)?.value).isEqualTo("graphite.default.svc.cluster.local")
        assertThat(container?.volumeMounts).hasSize(4)
        assertThat(container?.volumeMounts?.get(3)?.name).isEqualTo("taskmanager")
        assertThat(container?.resources).isEqualTo(cluster.spec.taskManager.resources)
    }
}
