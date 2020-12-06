package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class BatchJobDeleteTest {
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = BatchJobDelete(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.deleteJob(eq("flink"), eq("xxx"))).thenThrow(RuntimeException::class.java)
        val result = command.execute("flink", "test", "test", "xxx")
        verify(kubeClient, times(1)).deleteJob(eq("flink"), eq("xxx"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should delete bootstrap job`() {
        val result = command.execute("flink", "test", "test", "xxx")
        verify(kubeClient, times(1)).deleteJob(eq("flink"), eq("xxx"))
//        verify(kubeClient, times(1)).deleteBootstrapPod(eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
    }
}