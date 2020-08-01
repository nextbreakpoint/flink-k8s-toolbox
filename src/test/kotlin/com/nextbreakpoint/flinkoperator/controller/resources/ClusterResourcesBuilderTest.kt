package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import io.kubernetes.client.models.V1Pod
import io.kubernetes.client.models.V1Service
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import java.util.UUID

class ClusterResourcesBuilderTest {
    private val factory = mock(ClusterResourcesFactory::class.java)

    private val jobmanagerService = mock(V1Service::class.java)
    private val jobmanagerPod = mock(V1Pod::class.java)
    private val taskmanagerPod = mock(V1Pod::class.java)

    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")

    private val clusterSelector = UUID.randomUUID().toString()

    private val builder = ClusterResourcesBuilder(
        factory, "test", clusterSelector, "myself", cluster
    )

    @BeforeEach
    fun setup() {
        given(factory.createService(any(), any(), any(), any())).thenReturn(jobmanagerService)
        given(factory.createJobManagerPod(any(), any(), any(), any())).thenReturn(jobmanagerPod)
        given(factory.createTaskManagerPod(any(), any(), any(), any())).thenReturn(taskmanagerPod)
    }

    @Test
    fun `should invoke factory to create resources`() {
        val resources = builder.build()

        assertThat(resources).isNotNull()
        assertThat(resources.service).isEqualTo(jobmanagerService)
        assertThat(resources.jobmanagerPod).isEqualTo(jobmanagerPod)
        assertThat(resources.taskmanagerPod).isEqualTo(taskmanagerPod)

        verify(factory, times(1)).createService(eq("test"), eq(clusterSelector), eq("myself"), eq(cluster))
        verify(factory, times(1)).createJobManagerPod(eq("test"), eq(clusterSelector), eq("myself"), eq(cluster))
        verify(factory, times(1)).createTaskManagerPod(eq("test"), eq(clusterSelector), eq("myself"), eq(cluster))
    }
}