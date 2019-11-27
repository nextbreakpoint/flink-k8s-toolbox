package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkclient.model.ClusterOverviewWithVersion
import com.nextbreakpoint.flinkclient.model.JarEntryInfo
import com.nextbreakpoint.flinkclient.model.JarFileInfo
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
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

class JobStartTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val flinkAddress = FlinkAddress(host = "localhost", port = 8080)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = JobStart(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenReturn(flinkAddress)
        given(flinkClient.listJars(eq(flinkAddress))).thenReturn(listOf())
        val overview = ClusterOverviewWithVersion()
        overview.slotsTotal = 0
        overview.taskmanagers = 0
        overview.jobsRunning = 0
        overview.jobsFinished = 0
        given(flinkClient.getOverview(eq(flinkAddress))).thenReturn(overview)
        Status.setSavepointPath(cluster, null)
        Status.setJobParallelism(cluster, 1)
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, cluster)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should fail when flinkClient throws exception`() {
        given(flinkClient.listJars(eq(flinkAddress))).thenThrow(RuntimeException::class.java)
        val result = command.execute(clusterId, cluster)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verify(flinkClient, times(1)).listJars(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.FAILED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there are jobs running`() {
        val overview = ClusterOverviewWithVersion()
        overview.slotsTotal = 0
        overview.taskmanagers = 0
        overview.jobsRunning = 1
        overview.jobsFinished = 0
        given(flinkClient.getOverview(eq(flinkAddress))).thenReturn(overview)
        given(flinkClient.listRunningJobs(eq(flinkAddress))).thenReturn(listOf("aJob"))
        val result = command.execute(clusterId, cluster)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there aren't jar files`() {
        given(flinkClient.listJars(eq(flinkAddress))).thenReturn(listOf())
        val result = command.execute(clusterId, cluster)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verify(flinkClient, times(1)).listJars(eq(flinkAddress))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.RETRY)
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
        given(flinkClient.listJars(eq(flinkAddress))).thenReturn(listOf(file1, file2))
        val result = command.execute(clusterId, cluster)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verify(flinkClient, times(1)).listJars(eq(flinkAddress))
        verify(flinkClient, times(1)).runJar(
            eq(flinkAddress),
            eq(file2),
            any(),
            Mockito.eq(1),
            eq(""),
            any()
        )
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should return expected result when there are jar files and there is a savepoint`() {
        Status.setSavepointPath(cluster, "/tmp/000")
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
        given(flinkClient.listJars(eq(flinkAddress))).thenReturn(listOf(file1, file2))
        val result = command.execute(clusterId, cluster)
        verify(kubeClient, times(1)).findFlinkAddress(eq(flinkOptions), eq("flink"), eq("test"))
        verify(flinkClient, times(1)).getOverview(eq(flinkAddress))
        verify(flinkClient, times(1)).listJars(eq(flinkAddress))
        verify(flinkClient, times(1)).runJar(
            eq(flinkAddress),
            eq(file2),
            any(),
            Mockito.eq(1),
            eq("/tmp/000"),
            any()
        )
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(OperationStatus.COMPLETED)
        assertThat(result.output).isNull()
    }
}