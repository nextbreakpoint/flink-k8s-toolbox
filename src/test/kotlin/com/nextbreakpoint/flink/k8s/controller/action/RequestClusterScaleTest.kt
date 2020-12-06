package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.FlinkClusterAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class RequestClusterScaleTest {
    private val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val supervisorCache = mock(Cache::class.java)
    private val command = RequestClusterScale(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Started)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.rescaleCluster(eq("flink"), eq("test"), eq(4))).thenThrow(RuntimeException::class.java)
        val result = command.execute("flink", "test", ScaleClusterOptions(taskManagers = 4))
        verify(kubeClient, times(1)).rescaleCluster(eq("flink"), eq("test"), eq(4))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(supervisorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when scaling`() {
        val actionTimestamp = FlinkClusterAnnotations.getActionTimestamp(cluster)
        val result = command.execute("flink", "test", ScaleClusterOptions(taskManagers = 4))
        verify(kubeClient, times(1)).rescaleCluster(eq("flink"), eq("test"), eq(4))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(supervisorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isEqualTo(actionTimestamp)
    }

    @Test
    fun `should return expected result when scaling down to zero`() {
        val actionTimestamp = FlinkClusterAnnotations.getActionTimestamp(cluster)
        val result = command.execute("flink", "test", ScaleClusterOptions(taskManagers = 0))
        verify(kubeClient, times(1)).rescaleCluster(eq("flink"), eq("test"), eq(0))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        verifyNoMoreInteractions(supervisorCache)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isEqualTo(actionTimestamp)
    }
}