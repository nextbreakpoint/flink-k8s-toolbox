package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
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

class SavepointForgetTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val adapter = CacheAdapter(cluster, CachedResources())
    private val command = SavepointForget(flinkOptions, flinkClient, kubeClient, adapter)

    @BeforeEach
    fun configure() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.appendTasks(cluster, listOf(ClusterTask.ClusterHalted))
    }

    @Test
    fun `should forget savepoint when status is failed`() {
        Status.setClusterStatus(cluster, ClusterStatus.Failed)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.EraseSavepoint,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should forget savepoint when status is suspended`() {
        Status.setClusterStatus(cluster, ClusterStatus.Suspended)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.EraseSavepoint,
            ClusterTask.ClusterHalted
        ))
    }

    @Test
    fun `should forget savepoint when status is terminated`() {
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        val result = command.execute(clusterId, null)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).containsExactlyElementsOf(listOf(
            ClusterTask.EraseSavepoint,
            ClusterTask.ClusterHalted
        ))
    }
}