package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class ClusterScaleTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val operatorCache = mock(OperatorCache::class.java)
    private val command = ClusterScale(flinkOptions, flinkContext, kubernetesContext, operatorCache)

    @BeforeEach
    fun configure() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Running)
        OperatorState.setTaskStatus(cluster, TaskStatus.Idle)
        OperatorState.appendTasks(cluster, listOf(ClusterTask.ClusterRunning))
        given(operatorCache.getFlinkCluster(eq(clusterId))).thenReturn(cluster)
    }

    @Test
    fun `should fail when cluster doesn't exist`() {
        given(operatorCache.getFlinkCluster(eq(clusterId))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 4, taskSlots = 2))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isEmpty()
    }

    @Test
    fun `should return expected result when operator is not idle`() {
        OperatorState.setTaskStatus(cluster, TaskStatus.Awaiting)
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 4, taskSlots = 2))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(
            listOf(
                ClusterTask.ClusterRunning
            )
        )
    }

    @Test
    fun `should return expected result when operator is idle but cluster is not running`() {
        OperatorState.setClusterStatus(cluster, ClusterStatus.Suspended)
        OperatorState.resetTasks(cluster, listOf(ClusterTask.ClusterHalted))
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 4, taskSlots = 2))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).containsExactlyElementsOf(
            listOf(
                ClusterTask.ClusterHalted
            )
        )
    }

    @Test
    fun `should return expected result when job is not defined and cluster is running`() {
        cluster.spec.bootstrap = null
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 4, taskSlots = 2))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(
            listOf(
                ClusterTask.RescaleCluster,
                ClusterTask.ClusterRunning
            )
        )
    }

    @Test
    fun `should return expected result when job is not defined and cluster is running and no task managers`() {
        cluster.spec.bootstrap = null
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 0, taskSlots = 2))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(
            listOf(
                ClusterTask.StoppingCluster,
                ClusterTask.TerminatePods,
                ClusterTask.SuspendCluster,
                ClusterTask.ClusterHalted
            )
        )
    }

    @Test
    fun `should return expected result when job is defined and cluster is running`() {
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 4, taskSlots = 2))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(
            listOf(
                ClusterTask.StoppingCluster,
                ClusterTask.CancelJob,
                ClusterTask.RescaleCluster,
                ClusterTask.StartingCluster,
                ClusterTask.DeleteBootstrapJob,
                ClusterTask.CreateBootstrapJob,
                ClusterTask.StartJob,
                ClusterTask.ClusterRunning
            )
        )
    }

    @Test
    fun `should return expected result when job is defined and cluster is running and no task managers`() {
        val result = command.execute(clusterId, ClusterScaling(taskManagers = 0, taskSlots = 2))
        verify(operatorCache, times(1)).getFlinkCluster(eq(clusterId))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).containsExactlyElementsOf(
            listOf(
                ClusterTask.StoppingCluster,
                ClusterTask.CancelJob,
                ClusterTask.TerminatePods,
                ClusterTask.SuspendCluster,
                ClusterTask.ClusterHalted
            )
        )
    }
}
