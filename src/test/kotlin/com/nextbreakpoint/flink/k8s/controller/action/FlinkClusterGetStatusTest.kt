package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterContext
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verifyNoMoreInteractions

class FlinkClusterGetStatusTest {
    private val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val context = ClusterContext(cluster)
    private val command = FlinkClusterGetStatus(flinkOptions, flinkClient, kubeClient, context)

    @BeforeEach
    fun configure() {
        FlinkClusterStatus.setSupervisorStatus(cluster, ClusterStatus.Started)
    }

    @Test
    fun `should return cluster status`() {
        val result = command.execute("flink", "test", null)
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNotNull()
        assertThat(result.output?.supervisorStatus).isEqualTo("Started")
    }
}