package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ResourceStatus
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.V1EnvVar
import io.kubernetes.client.models.V1ObjectMeta
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1ServiceSpec
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID

class ClusterResourcesValidatorTest {
    private val statusEvaluator = ClusterResourcesValidator()

    private val clusterSelector = UUID.randomUUID().toString()

    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")

    private val identity = ClusterSelector(
        namespace = "test",
        name = cluster.metadata.name,
        uuid = clusterSelector
    )

    @Test
    fun `should return all valid resources when creating resources from base configuration`() {
        val expectedResources = createTestClusterResources(cluster)

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.service.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.VALID)
    }

    @Test
    fun `should return missing resource when the job manager service is not present`() {
        val expectedResources = createTestClusterResources(cluster).withService(null)

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.service.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing resource when the job manager pod is not present`() {
        val expectedResources = createTestClusterResources(cluster).withJobManagerPod(null)

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing resource when the task manager pod is not present`() {
        val expectedResources = createTestClusterResources(cluster).withTaskManagerPod(null)

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return divergent resource when the job manager service does not have the expected labels`() {
        val jobmanagerService = V1Service()

        jobmanagerService.metadata = V1ObjectMeta()
        jobmanagerService.metadata.name = "jobmanager-test"
        jobmanagerService.metadata.namespace = "flink"
        jobmanagerService.metadata.labels = mapOf()

        jobmanagerService.spec = V1ServiceSpec()
        jobmanagerService.spec.type = "ClusterIP"

        val expectedResources = createTestClusterResources(cluster).withService(jobmanagerService)

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.service.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.service.second).hasSize(4)
    }

    @Test
    fun `should return divergent resource when the job manager service does not have the expected service mode`() {
        val jobmanagerService = V1Service()

        val labels = createLabels("flink-operator", "jobmanager", clusterSelector, cluster.metadata.name)

        jobmanagerService.metadata = V1ObjectMeta()
        jobmanagerService.metadata.name = "jobmanager-test"
        jobmanagerService.metadata.namespace = "flink"
        jobmanagerService.metadata.labels = labels

        jobmanagerService.spec = V1ServiceSpec()
        jobmanagerService.spec.type = "NodePort"

        val expectedResources = createTestClusterResources(cluster).withService(jobmanagerService)

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.service.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.service.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have the expected labels`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.metadata?.labels = mapOf()

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(4)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have the expected service account`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.spec?.serviceAccountName = "xxx"

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have the expected pull secrets`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.spec?.imagePullSecrets = listOf()

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have the expected pull secrets name`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.spec?.imagePullSecrets?.get(0)?.name = "xxx"

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have containers`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.spec?.containers = listOf()

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have the expected number of init containers`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.spec?.initContainers = listOf()

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have the expected container image`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.spec?.containers?.get(0)?.image = "xxx"

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have the expected container pull policy`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.spec?.containers?.get(0)?.imagePullPolicy = "xxx"

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have the expected container cpu limits`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.spec?.containers?.get(0)?.resources?.limits?.set("cpu", Quantity("2.0"))

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have the expected container memory limits`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.spec?.containers?.get(0)?.resources?.requests?.set("memory", Quantity("100Mi"))

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have the expected container environment variables`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.spec?.containers?.get(0)?.env = listOf()

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(4)
    }

    @Test
    fun `should return divergent resource when the job manager pod does not have the internal expected container environment variables`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.jobmanagerPod?.spec?.containers?.get(0)?.env = listOf(V1EnvVar().name("key").value("value"))

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPod.second).hasSize(4)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have the expected labels`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.metadata?.labels = mapOf()

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(4)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have the expected service account`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.spec?.serviceAccountName = "xxx"

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have the expected pull secrets`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.spec?.imagePullSecrets = listOf()

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have the expected pull secrets name`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.spec?.imagePullSecrets?.get(0)?.name = "xxx"

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have containers`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.spec?.containers = listOf()

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have the expected number of init containers`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.spec?.initContainers = listOf()

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have the expected container image`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.spec?.containers?.get(0)?.image = "xxx"

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have the expected container pull policy`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.spec?.containers?.get(0)?.imagePullPolicy = "xxx"

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have the expected container cpu limits`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.spec?.containers?.get(0)?.resources?.limits?.set("cpu", Quantity("2.0"))

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have the expected container memory limits`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.spec?.containers?.get(0)?.resources?.requests?.set("memory", Quantity("100Mi"))

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have the expected container environment variables`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.spec?.containers?.get(0)?.env = listOf()

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(5)
    }

    @Test
    fun `should return divergent resource when the task manager pod does not have the internal expected container environment variables`() {
        val expectedResources = createTestClusterResources(cluster)

        expectedResources.taskmanagerPod?.spec?.containers?.get(0)?.env = listOf(V1EnvVar().name("key").value("value"))

        val actualStatus = statusEvaluator.evaluate(identity, cluster, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPod.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPod.second).hasSize(5)
    }

    private fun printStatus(clusterResourcesStatus: ClusterResourcesStatus) {
        clusterResourcesStatus.service.second.forEach { println("jobmanager service: ${it}") }

        clusterResourcesStatus.jobmanagerPod.second.forEach { println("jobmanager pod: ${it}") }

        clusterResourcesStatus.taskmanagerPod.second.forEach { println("taskmanager pod: ${it}") }
    }

    private fun createLabels(
        clusterOwner: String,
        role: String,
        clusterSelector: String,
        clusterName: String
    ): Map<String, String> {
        val componentLabel = Pair("component", "flink")

        val clusterLabel = Pair("name", clusterName)

        val clusterSelectorLabel = Pair("uid", clusterSelector)

        val ownerLabel = Pair("owner", clusterOwner)

        val roleLabel = Pair("role", role)

        return mapOf(ownerLabel, clusterLabel, clusterSelectorLabel, componentLabel, roleLabel)
    }

    private fun createTestClusterResources(cluster: V1FlinkCluster): ClusterResources {
        val targetResources = ClusterResourcesBuilder(
            DefaultClusterResourcesFactory,
            "test",
            clusterSelector,
            "flink-operator",
            cluster
        ).build()

        return ClusterResources(
            service = targetResources.service,
            jobmanagerPod = targetResources.jobmanagerPod,
            taskmanagerPod = targetResources.taskmanagerPod
        )
    }
}