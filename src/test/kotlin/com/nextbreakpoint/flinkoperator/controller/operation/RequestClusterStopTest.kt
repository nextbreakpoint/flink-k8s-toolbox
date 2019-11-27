package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Annotations
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.CacheAdapter
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class RequestClusterStopTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val adapter = CacheAdapter(cluster, CachedResources())
    private val command = RequestClusterStop(flinkOptions, flinkClient, kubeClient, adapter)

    @BeforeEach
    fun configure() {
        Status.setClusterStatus(cluster, ClusterStatus.Terminated)
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.appendTasks(cluster, listOf(ClusterTask.ClusterRunning))
    }

    @Test
    fun `should return expected result when stopping without savepoint and not deleting resources`() {
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.setClusterStatus(cluster, ClusterStatus.Stopping)
        val actionTimestamp = Annotations.getActionTimestamp(cluster)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = true, deleteResources = false))
        verify(kubeClient, times(1)).updateAnnotations(eq(clusterId), KotlinMockito.any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isNull()
        assertThat(Annotations.getManualAction(cluster)).isEqualTo(ManualAction.STOP)
        assertThat(Annotations.isWithSavepoint(cluster)).isEqualTo(true)
        assertThat(Annotations.isDeleteResources(cluster)).isEqualTo(false)
        assertThat(Annotations.getActionTimestamp(cluster)).isNotEqualTo(actionTimestamp)
    }

    @Test
    fun `should return expected result when stopping with savepoint and deleting resources`() {
        Status.setTaskStatus(cluster, TaskStatus.Idle)
        Status.setClusterStatus(cluster, ClusterStatus.Stopping)
        val actionTimestamp = Annotations.getActionTimestamp(cluster)
        val result = command.execute(clusterId, StopOptions(withoutSavepoint = false, deleteResources = true))
        verify(kubeClient, times(1)).updateAnnotations(eq(clusterId), KotlinMockito.any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isNull()
        assertThat(Annotations.getManualAction(cluster)).isEqualTo(ManualAction.STOP)
        assertThat(Annotations.isWithSavepoint(cluster)).isEqualTo(false)
        assertThat(Annotations.isDeleteResources(cluster)).isEqualTo(true)
        assertThat(Annotations.getActionTimestamp(cluster)).isNotEqualTo(actionTimestamp)
    }
}