package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StartOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.CacheAdapter
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterStartTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val adapter = CacheAdapter(cluster, CachedResources())
    private val command = ClusterStart(flinkOptions, flinkClient, kubeClient, adapter)

    @BeforeEach
    fun configure() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.setBootstrap(cluster, cluster.spec.bootstrap)
        Status.appendTasks(cluster, listOf(ClusterTask.ClusterHalted))
    }

    @Test
    fun `should return expected result when job is not defined and cluster is terminated and savepoint is enabled`() {
        Status.setBootstrap(cluster,null)
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StartingCluster,
            ClusterTask.CreateResources,
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is not defined and cluster is suspended and savepoint is enabled`() {
        Status.setBootstrap(cluster,null)
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StartingCluster,
            ClusterTask.RestartPods,
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is not defined and cluster has failed and savepoint is enabled`() {
        Status.setBootstrap(cluster,null)
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.StartingCluster,
            ClusterTask.CreateResources,
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is not defined and cluster is checkpointing and savepoint is enabled`() {
        Status.setBootstrap(cluster,null)
        Status.setClusterStatus(cluster, ClusterStatus.Checkpointing)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isEmpty()
    }

    @Test
    fun `should return expected result when job is defined and cluster is terminated and savepoint is enabled`() {
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StartingCluster,
            ClusterTask.EraseSavepoint,
            ClusterTask.CreateResources,
            ClusterTask.CreateBootstrapJob,
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is suspended and savepoint is enabled`() {
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StartingCluster,
            ClusterTask.EraseSavepoint,
            ClusterTask.RestartPods,
            ClusterTask.CreateBootstrapJob,
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster has failed and savepoint is enabled`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.StartingCluster,
            ClusterTask.EraseSavepoint,
            ClusterTask.CreateResources,
            ClusterTask.CreateBootstrapJob,
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is checkpointing and savepoint is enabled`() {
        Status.setClusterStatus(cluster, ClusterStatus.Checkpointing)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = true))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isEmpty()
    }

    @Test
    fun `should return expected result when job is defined and cluster is terminated and savepoint is disabled`() {
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StartingCluster,
            ClusterTask.CreateResources,
            ClusterTask.CreateBootstrapJob,
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is suspended and savepoint is disabled`() {
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StartingCluster,
            ClusterTask.RestartPods,
            ClusterTask.CreateBootstrapJob,
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster has failed and savepoint is disabled`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.StoppingCluster,
            ClusterTask.TerminatePods,
            ClusterTask.StartingCluster,
            ClusterTask.CreateResources,
            ClusterTask.CreateBootstrapJob,
            ClusterTask.ClusterRunning
        ))
    }

    @Test
    fun `should return expected result when job is defined and cluster is checkpointing and savepoint is disabled`() {
        Status.setClusterStatus(cluster, ClusterStatus.Checkpointing)
        val result = command.execute(clusterId, StartOptions(withoutSavepoint = false))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isEmpty()
    }
}