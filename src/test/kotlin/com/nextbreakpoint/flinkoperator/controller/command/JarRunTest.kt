package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkclient.model.ClusterOverviewWithVersion
import com.nextbreakpoint.flinkclient.model.JarEntryInfo
import com.nextbreakpoint.flinkclient.model.JarFileInfo
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
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

class JarRunTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkContext = mock(FlinkContext::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val kubernetesContext = mock(KubernetesContext::class.java)
    private val command = JarRun(flinkOptions, flinkContext, kubernetesContext)

    @BeforeEach
    fun configure() {
        given(kubernetesContext.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        given(flinkContext.listJars(eq(flinkAddress))).thenReturn(listOf())
        val overview = ClusterOverviewWithVersion()
        overview.slotsTotal = 0
        overview.taskmanagers = 0
        overview.jobsRunning = 0
        overview.jobsFinished = 0
        given(flinkContext.getOverview(eq(flinkAddress))).thenReturn(overview)
    }

    @Test
    fun `should fail when kubernetesContext throws exception`() {
        given(kubernetesContext.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, cluster)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should fail when flinkContext throws exception`() {
        given(flinkContext.listJars(eq(flinkAddress))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, cluster)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).getOverview(eq(flinkAddress))
        verify(flinkContext, times(1)).listJars(eq(flinkAddress))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there are jobs running`() {
        val overview = ClusterOverviewWithVersion()
        overview.slotsTotal = 0
        overview.taskmanagers = 0
        overview.jobsRunning = 1
        overview.jobsFinished = 0
        given(flinkContext.getOverview(eq(flinkAddress))).thenReturn(overview)
        given(flinkContext.listRunningJobs(eq(flinkAddress))).thenReturn(listOf("aJob"))
        val result = command.execute(clusterId, cluster)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).getOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there aren't jar files`() {
        given(flinkContext.listJars(eq(flinkAddress))).thenReturn(listOf())
        val result = command.execute(clusterId, cluster)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).getOverview(eq(flinkAddress))
        verify(flinkContext, times(1)).listJars(eq(flinkAddress))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there are jar files`() {
        val file1 = JarFileInfo()
        file1.id = "id1"
        file1.name = "file1"
        file1.uploaded = 0
        file1.addEntryItem(JarEntryInfo())
        val file2 = JarFileInfo()
        file2.id = "id2"
        file2.name = "file2"
        file2.uploaded = 1
        file2.addEntryItem(JarEntryInfo())
        given(flinkContext.listJars(eq(flinkAddress))).thenReturn(listOf(file1, file2))
        val result = command.execute(clusterId, cluster)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).getOverview(eq(flinkAddress))
        verify(flinkContext, times(1)).listJars(eq(flinkAddress))
        verify(flinkContext, times(1)).runJar(eq(flinkAddress), eq(file2), any(), eq(null))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there are jar files and there is a savepoint`() {
        cluster.spec.flinkOperator.savepointPath = "/tmp/000"
        val file1 = JarFileInfo()
        file1.id = "id1"
        file1.name = "file1"
        file1.uploaded = 0
        file1.addEntryItem(JarEntryInfo())
        val file2 = JarFileInfo()
        file2.id = "id2"
        file2.name = "file2"
        file2.uploaded = 1
        file2.addEntryItem(JarEntryInfo())
        given(flinkContext.listJars(eq(flinkAddress))).thenReturn(listOf(file1, file2))
        val result = command.execute(clusterId, cluster)
        verify(kubernetesContext, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkContext, times(1)).getOverview(eq(flinkAddress))
        verify(flinkContext, times(1)).listJars(eq(flinkAddress))
        verify(flinkContext, times(1)).runJar(eq(flinkAddress), eq(file2), any(), eq("/tmp/000"))
        verifyNoMoreInteractions(kubernetesContext)
        verifyNoMoreInteractions(flinkContext)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNull()
    }
}