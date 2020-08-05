package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.ScaleOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.common.Annotations
import com.nextbreakpoint.flinkoperator.server.supervisor.core.Cache
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import com.nextbreakpoint.flinkoperator.server.common.Status
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
    private val supervisorCache = mock(Cache::class.java)
    private val command = RequestClusterScale(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        Status.setClusterStatus(cluster, ClusterStatus.Running)
        given(supervisorCache.getFlinkCluster(eq(clusterSelector))).thenReturn(cluster)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.rescaleCluster(eq(clusterSelector), Mockito.eq(4))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, ScaleOptions(taskManagers = 4))
        verify(kubeClient, times(1)).rescaleCluster(eq(clusterSelector), Mockito.eq(4))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(supervisorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when scaling`() {
        val actionTimestamp = Annotations.getActionTimestamp(cluster)
        val result = command.execute(clusterSelector, ScaleOptions(taskManagers = 4))
        verify(kubeClient, times(1)).rescaleCluster(eq(clusterSelector), Mockito.eq(4))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(supervisorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
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
        verifyNoMoreInteractions(supervisorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
        assertThat(Annotations.getActionTimestamp(cluster)).isEqualTo(actionTimestamp)
    }
}