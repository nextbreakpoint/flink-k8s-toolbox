package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ScaleOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Annotations
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class RequestClusterScaleTest {
    private val clusterSelector = ClusterSelector(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val operatorCache = mock(Cache::class.java)
    private val command = RequestClusterScale(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        given(operatorCache.getFlinkCluster(eq(clusterSelector))).thenReturn(cluster)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.rescaleCluster(eq(clusterSelector), Mockito.eq(4))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, ScaleOptions(taskManagers = 4))
        verify(kubeClient, times(1)).rescaleCluster(eq(clusterSelector), Mockito.eq(4))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when scaling`() {
        val actionTimestamp = Annotations.getActionTimestamp(cluster)
        val result = command.execute(clusterSelector, ScaleOptions(taskManagers = 4))
        verify(kubeClient, times(1)).rescaleCluster(eq(clusterSelector), Mockito.eq(4))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.OK)
        assertThat(result.output).isNull()
        assertThat(Annotations.getActionTimestamp(cluster)).isEqualTo(actionTimestamp)
    }

    @Test
    fun `should return expected result when scaling down to zero`() {
        val actionTimestamp = Annotations.getActionTimestamp(cluster)
        val result = command.execute(clusterSelector, ScaleOptions(taskManagers = 0))
        verify(kubeClient, times(1)).rescaleCluster(eq(clusterSelector), Mockito.eq(0))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(operatorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.OK)
        assertThat(result.output).isNull()
        assertThat(Annotations.getActionTimestamp(cluster)).isEqualTo(actionTimestamp)
    }
}