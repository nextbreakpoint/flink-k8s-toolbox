package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
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
    private val cluster1 = TestFactory.aCluster(name = "test", namespace = "flink")
    private val cluster2 = TestFactory.aCluster(name = "test", namespace = "flink")

    @BeforeEach
    fun setup() {
        cluster1.metadata.uid = "123"
        cluster2.metadata.uid = "456"
    }

    @Test
    fun `should watch flink cluster resources`() {
        val watch = TestWatchable(listOf(
            Watch.Response("ADDED", cluster1),
            Watch.Response("ADDED", cluster2),
            Watch.Response("DELETED", cluster1),
            Watch.Response("MODIFIED", cluster2)
        ).toMutableList().iterator())

        given(kubeClient.watchFlickClusters("flink")).thenThrow(RuntimeException("temporary error")).thenReturn(watch)

        val thread = adapter.watchClusters("flink")

        Thread.sleep(100)

        verify(kubeClient, times(1)).watchFlickClusters(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        verify(cache, times(1)).onFlinkClusterDeletedAll()
        verifyNoMoreInteractions(cache)

        Thread.sleep(1000)
        thread.interrupt()
        thread.join()

        verify(kubeClient, times(2)).watchFlickClusters(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        val inOrder = inOrder(cache)
        inOrder.verify(cache, times(2)).onFlinkClusterDeletedAll()
        inOrder.verify(cache, times(1)).onFlinkClusterChanged(eq(cluster1))
        inOrder.verify(cache, times(1)).onFlinkClusterChanged(eq(cluster2))
        inOrder.verify(cache, times(1)).onFlinkClusterDeleted(eq(cluster1))
        inOrder.verify(cache, times(1)).onFlinkClusterChanged(eq(cluster2))
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should watch job resources`() {
        val resource1 = TestFactory.aBootstrapJob(cluster1)
        val resource2 = TestFactory.aBootstrapJob(cluster2)

        val watch = TestWatchable(listOf(
            Watch.Response("ADDED", resource1),
            Watch.Response("ADDED", resource2),
            Watch.Response("DELETED", resource1),
            Watch.Response("MODIFIED", resource2)
        ).toMutableList().iterator())

        given(kubeClient.watchJobs("flink")).thenThrow(RuntimeException("temporary error")).thenReturn(watch)

        val thread = adapter.watchJobs("flink")

        Thread.sleep(100)

        verify(kubeClient, times(1)).watchJobs(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        verify(cache, times(1)).onJobDeletedAll()
        verifyNoMoreInteractions(cache)

        Thread.sleep(1000)
        thread.interrupt()
        thread.join()

        verify(kubeClient, times(2)).watchJobs(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        val inOrder = inOrder(cache)
        inOrder.verify(cache, times(2)).onJobDeletedAll()
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

        val watch = TestWatchable(listOf(
            Watch.Response("ADDED", resource1),
            Watch.Response("ADDED", resource2),
            Watch.Response("DELETED", resource1),
            Watch.Response("MODIFIED", resource2)
        ).toMutableList().iterator())

        given(kubeClient.watchServices("flink")).thenThrow(RuntimeException("temporary error")).thenReturn(watch)

        val thread = adapter.watchServices("flink")

        Thread.sleep(100)

        verify(kubeClient, times(1)).watchServices(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        verify(cache, times(1)).onServiceDeletedAll()
        verifyNoMoreInteractions(cache)

        Thread.sleep(1000)
        thread.interrupt()
        thread.join()

        verify(kubeClient, times(2)).watchServices(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        val inOrder = inOrder(cache)
        inOrder.verify(cache, times(2)).onServiceDeletedAll()
        inOrder.verify(cache, times(1)).onServiceChanged(eq(resource1))
        inOrder.verify(cache, times(1)).onServiceChanged(eq(resource2))
        inOrder.verify(cache, times(1)).onServiceDeleted(eq(resource1))
        inOrder.verify(cache, times(1)).onServiceChanged(eq(resource2))
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should watch statefulset resources`() {
        val resource1 = TestFactory.aJobManagerStatefulSet(cluster1)
        val resource2 = TestFactory.aTaskManagerStatefulSet(cluster2)

        val watch = TestWatchable(listOf(
            Watch.Response("ADDED", resource1),
            Watch.Response("ADDED", resource2),
            Watch.Response("DELETED", resource1),
            Watch.Response("MODIFIED", resource2)
        ).toMutableList().iterator())

        given(kubeClient.watchStatefulSets("flink")).thenThrow(RuntimeException("temporary error")).thenReturn(watch)

        val thread = adapter.watchStatefuleSets("flink")

        Thread.sleep(100)

        verify(kubeClient, times(1)).watchStatefulSets(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        verify(cache, times(1)).onStatefulSetDeletedAll()
        verifyNoMoreInteractions(cache)

        Thread.sleep(1000)
        thread.interrupt()
        thread.join()

        verify(kubeClient, times(2)).watchStatefulSets(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        val inOrder = inOrder(cache)
        inOrder.verify(cache, times(2)).onStatefulSetDeletedAll()
        inOrder.verify(cache, times(1)).onStatefulSetChanged(eq(resource1))
        inOrder.verify(cache, times(1)).onStatefulSetChanged(eq(resource2))
        inOrder.verify(cache, times(1)).onStatefulSetDeleted(eq(resource1))
        inOrder.verify(cache, times(1)).onStatefulSetChanged(eq(resource2))
        inOrder.verifyNoMoreInteractions()
    }

    @Test
    fun `should watch persistent volume claim resources`() {
        val resource1 = TestFactory.aJobManagerPersistenVolumeClaim(cluster1)
        val resource2 = TestFactory.aTaskManagerPersistenVolumeClaim(cluster2)

        val watch = TestWatchable(listOf(
            Watch.Response("ADDED", resource1),
            Watch.Response("ADDED", resource2),
            Watch.Response("DELETED", resource1),
            Watch.Response("MODIFIED", resource2)
        ).toMutableList().iterator())

        given(kubeClient.watchPersistentVolumeClaims("flink")).thenThrow(RuntimeException("temporary error")).thenReturn(watch)

        val thread = adapter.watchPersistentVolumeClaims("flink")

        Thread.sleep(100)

        verify(kubeClient, times(1)).watchPersistentVolumeClaims(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        verify(cache, times(1)).onPersistentVolumeClaimDeletedAll()
        verifyNoMoreInteractions(cache)

        Thread.sleep(1000)
        thread.interrupt()
        thread.join()

        verify(kubeClient, times(2)).watchPersistentVolumeClaims(eq("flink"))
        verifyNoMoreInteractions(kubeClient)

        val inOrder = inOrder(cache)
        inOrder.verify(cache, times(2)).onPersistentVolumeClaimDeletedAll()
        inOrder.verify(cache, times(1)).onPersistentVolumeClaimChanged(eq(resource1))
        inOrder.verify(cache, times(1)).onPersistentVolumeClaimChanged(eq(resource2))
        inOrder.verify(cache, times(1)).onPersistentVolumeClaimDeleted(eq(resource1))
        inOrder.verify(cache, times(1)).onPersistentVolumeClaimChanged(eq(resource2))
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
