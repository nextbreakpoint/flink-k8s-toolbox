package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.testing.KotlinMockito.given
import com.nextbreakpoint.flink.testing.TestFactory
import io.kubernetes.client.util.Watch
import io.kubernetes.client.util.Watchable
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class CacheAdpterTest {
    private val kubeClient = mock(KubeClient::class.java)
    private val cache = mock(Cache::class.java)
    private val adapter = CacheAdapter(kubeClient, cache, backoffTime = 1000)
    private val cluster1 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
    private val cluster2 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
    private val job1 = TestFactory.aFlinkJob(name = "test-test-1", namespace = "flink")
    private val job2 = TestFactory.aFlinkJob(name = "test-test-2", namespace = "flink")

    @BeforeEach
    fun setup() {
        cluster1.metadata.uid = "123"
        cluster2.metadata.uid = "456"
    }

    @Test
    fun `should watch flink cluster resources`() {
        val watch = TestWatchable(
            listOf(
                Watch.Response("ADDED", cluster1),
                Watch.Response("ADDED", cluster2),
                Watch.Response("DELETED", cluster1),
                Watch.Response("MODIFIED", cluster2)
            ).toMutableList().iterator()
        )

        given(kubeClient.watchFlickClusters("flink")).thenThrow(RuntimeException("temporary error")).thenReturn(watch)

        val thread = adapter.watchFlinkClusters("flink")

        Thread.sleep(100)

        verify(kubeClient, times(1)).watchFlickClusters(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        verify(cache, times(1)).onFlinkClustersReset()
        verifyNoMoreInteractions(cache)

        Thread.sleep(1000)
        thread.interrupt()
        thread.join()

        verify(kubeClient, times(2)).watchFlickClusters(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        val inOrder = inOrder(cache)
        inOrder.verify(cache, times(2)).onFlinkClustersReset()
        inOrder.verify(cache, times(1)).onFlinkClusterChanged(eq(cluster1))
        inOrder.verify(cache, times(1)).onFlinkClusterChanged(eq(cluster2))
        inOrder.verify(cache, times(1)).onFlinkClusterDeleted(eq(cluster1))
        inOrder.verify(cache, times(1)).onFlinkClusterChanged(eq(cluster2))
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should watch job resources`() {
        val resource1 = TestFactory.aBootstrapJob(cluster1, job1)
        val resource2 = TestFactory.aBootstrapJob(cluster2, job2)

        val watch = TestWatchable(
            listOf(
                Watch.Response("ADDED", resource1),
                Watch.Response("ADDED", resource2),
                Watch.Response("DELETED", resource1),
                Watch.Response("MODIFIED", resource2)
            ).toMutableList().iterator()
        )

        given(kubeClient.watchJobs("flink")).thenThrow(RuntimeException("temporary error")).thenReturn(watch)

        val thread = adapter.watchJobs("flink")

        Thread.sleep(100)

        verify(kubeClient, times(1)).watchJobs(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        verify(cache, times(1)).onJobsReset()
        verifyNoMoreInteractions(cache)

        Thread.sleep(1000)
        thread.interrupt()
        thread.join()

        verify(kubeClient, times(2)).watchJobs(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        val inOrder = inOrder(cache)
        inOrder.verify(cache, times(2)).onJobsReset()
        inOrder.verify(cache, times(1)).onJobChanged(eq(resource1))
        inOrder.verify(cache, times(1)).onJobChanged(eq(resource2))
        inOrder.verify(cache, times(1)).onJobDeleted(eq(resource1))
        inOrder.verify(cache, times(1)).onJobChanged(eq(resource2))
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should watch service resources`() {
        val resource1 = TestFactory.aJobManagerService(cluster1)
        val resource2 = TestFactory.aJobManagerService(cluster2)

        val watch = TestWatchable(
            listOf(
                Watch.Response("ADDED", resource1),
                Watch.Response("ADDED", resource2),
                Watch.Response("DELETED", resource1),
                Watch.Response("MODIFIED", resource2)
            ).toMutableList().iterator()
        )

        given(kubeClient.watchServices("flink")).thenThrow(RuntimeException("temporary error")).thenReturn(watch)

        val thread = adapter.watchServices("flink")

        Thread.sleep(100)

        verify(kubeClient, times(1)).watchServices(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        verify(cache, times(1)).onServicesReset()
        verifyNoMoreInteractions(cache)

        Thread.sleep(1000)
        thread.interrupt()
        thread.join()

        verify(kubeClient, times(2)).watchServices(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        val inOrder = inOrder(cache)
        inOrder.verify(cache, times(2)).onServicesReset()
        inOrder.verify(cache, times(1)).onServiceChanged(eq(resource1))
        inOrder.verify(cache, times(1)).onServiceChanged(eq(resource2))
        inOrder.verify(cache, times(1)).onServiceDeleted(eq(resource1))
        inOrder.verify(cache, times(1)).onServiceChanged(eq(resource2))
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should watch pod resources`() {
        val resource1 = TestFactory.aJobManagerPod(cluster1,"1")
        val resource2 = TestFactory.aTaskManagerPod(cluster2,"1")

        val watch = TestWatchable(
            listOf(
                Watch.Response("ADDED", resource1),
                Watch.Response("ADDED", resource2),
                Watch.Response("DELETED", resource1),
                Watch.Response("MODIFIED", resource2)
            ).toMutableList().iterator()
        )

        given(kubeClient.watchPods("flink")).thenThrow(RuntimeException("temporary error")).thenReturn(watch)

        val thread = adapter.watchPods("flink")

        Thread.sleep(100)

        verify(kubeClient, times(1)).watchPods(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        verify(cache, times(1)).onPodsReset()
        verifyNoMoreInteractions(cache)

        Thread.sleep(1000)
        thread.interrupt()
        thread.join()

        verify(kubeClient, times(2)).watchPods(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        val inOrder = inOrder(cache)
        inOrder.verify(cache, times(2)).onPodsReset()
        inOrder.verify(cache, times(1)).onPodChanged(eq(resource1))
        inOrder.verify(cache, times(1)).onPodChanged(eq(resource2))
        inOrder.verify(cache, times(1)).onPodDeleted(eq(resource1))
        inOrder.verify(cache, times(1)).onPodChanged(eq(resource2))
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should watch link job resources`() {
        val watch = TestWatchable(
            listOf(
                Watch.Response("ADDED", job1),
                Watch.Response("ADDED", job2),
                Watch.Response("DELETED", job1),
                Watch.Response("MODIFIED", job2)
            ).toMutableList().iterator()
        )

        given(kubeClient.watchFlinkJobs("flink")).thenThrow(RuntimeException("temporary error")).thenReturn(watch)

        val thread = adapter.watchFlinkJobs("flink")

        Thread.sleep(100)

        verify(kubeClient, times(1)).watchFlinkJobs(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        verify(cache, times(1)).onFlinkJobsReset()
        verifyNoMoreInteractions(cache)

        Thread.sleep(1000)
        thread.interrupt()
        thread.join()

        verify(kubeClient, times(2)).watchFlinkJobs(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        val inOrder = inOrder(cache)
        inOrder.verify(cache, times(2)).onFlinkJobsReset()
        inOrder.verify(cache, times(1)).onFlinkJobChanged(eq(job1))
        inOrder.verify(cache, times(1)).onFlinkJobChanged(eq(job2))
        inOrder.verify(cache, times(1)).onFlinkJobDeleted(eq(job1))
        inOrder.verify(cache, times(1)).onFlinkJobChanged(eq(job2))
        inOrder.verifyNoMoreInteractions()
    }

    class TestWatchable<T>(val elements: MutableIterator<Watch.Response<T>>) : Watchable<T> {
        override fun hasNext() = elements.hasNext()

        override fun close() {}

        override fun next() = elements.next()

        override fun iterator() = elements

        override fun remove() {
            elements.remove()
        }
    }
}
