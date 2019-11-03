package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import java.util.UUID

class ClusterResourcesBuilderTest {
    private val factory = mock(ClusterResourcesFactory::class.java)

    private val jarUploadJob = mock(V1Job::class.java)
    private val jobmanagerService = mock(V1Service::class.java)
    private val jobmanagerStatefulSet = mock(V1StatefulSet::class.java)
    private val taskmanagerStatefulSet = mock(V1StatefulSet::class.java)

    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")

    private val clusterId = UUID.randomUUID().toString()

    private val builder = ClusterResourcesBuilder(
        factory, "test", clusterId, "myself", cluster
    )

    @BeforeEach
    fun setup() {
        given(factory.createJarUploadJob(any(), any(), any(), any())).thenReturn(jarUploadJob)
        given(factory.createJobManagerService(any(), any(), any(), any())).thenReturn(jobmanagerService)
        given(factory.createJobManagerStatefulSet(any(), any(), any(), any())).thenReturn(jobmanagerStatefulSet)
        given(factory.createTaskManagerStatefulSet(any(), any(), any(), any())).thenReturn(taskmanagerStatefulSet)
    }

    @Test
    fun `should invoke factory to create resources`() {
        val resources = builder.build()

        assertThat(resources).isNotNull()
        assertThat(resources.jarUploadJob).isEqualTo(jarUploadJob)
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)

        verify(factory, times(1)).createJarUploadJob(eq("test"), eq(clusterId), eq("myself"), eq(cluster))
        verify(factory, times(1)).createJobManagerService(eq("test"), eq(clusterId), eq("myself"), eq(cluster))
        verify(factory, times(1)).createJobManagerStatefulSet(eq("test"), eq(clusterId), eq("myself"), eq(cluster))
        verify(factory, times(1)).createTaskManagerStatefulSet(eq("test"), eq(clusterId), eq("myself"), eq(cluster))
    }
}