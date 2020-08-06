package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.FlinkClusterAnnotations
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterContext
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class RequestClusterStopTest {
    private val clusterSelector = ResourceSelector(namespace = "flink", name = "test", uid = "123")
    private val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val context = ClusterContext(cluster)
    private val command = RequestClusterStop(flinkOptions, flinkClient, kubeClient, context)

    @BeforeEach
    fun configure() {
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Terminated)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        KotlinMockito.given(kubeClient.updateClusterAnnotations(eq(clusterSelector), KotlinMockito.any())).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterSelector, StopOptions(withoutSavepoint = true))
        verify(kubeClient, times(1)).updateClusterAnnotations(eq(clusterSelector), KotlinMockito.any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when stopping without savepoint`() {
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Stopping)
        val actionTimestamp = FlinkClusterAnnotations.getActionTimestamp(cluster)
        val result = command.execute(clusterSelector, StopOptions(withoutSavepoint = true))
        verify(kubeClient, times(1)).updateClusterAnnotations(eq(clusterSelector), KotlinMockito.any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
        assertThat(FlinkClusterAnnotations.getManualAction(cluster)).isEqualTo(ManualAction.STOP)
        assertThat(FlinkClusterAnnotations.isWithoutSavepoint(cluster)).isEqualTo(true)
        assertThat(FlinkClusterAnnotations.isDeleteResources(cluster)).isEqualTo(false)
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isNotEqualTo(actionTimestamp)
    }

    @Test
    fun `should return expected result when stopping with savepoint`() {
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Stopping)
        val actionTimestamp = FlinkClusterAnnotations.getActionTimestamp(cluster)
        val result = command.execute(clusterSelector, StopOptions(withoutSavepoint = false))
        verify(kubeClient, times(1)).updateClusterAnnotations(eq(clusterSelector), KotlinMockito.any())
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
        assertThat(FlinkClusterAnnotations.getManualAction(cluster)).isEqualTo(ManualAction.STOP)
        assertThat(FlinkClusterAnnotations.isWithoutSavepoint(cluster)).isEqualTo(false)
        assertThat(FlinkClusterAnnotations.isDeleteResources(cluster)).isEqualTo(false)
        assertThat(FlinkClusterAnnotations.getActionTimestamp(cluster)).isNotEqualTo(actionTimestamp)
    }
}