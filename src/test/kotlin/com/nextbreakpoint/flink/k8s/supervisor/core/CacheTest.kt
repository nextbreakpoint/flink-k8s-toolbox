package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class CacheTest {
    private val cache = Cache(namespace = "flink", clusterName = "test")

    @Test
    fun `cache is empty`() {
        assertThat(cache.getFlinkClusters()).isEmpty()
    }

    @Test
    fun `should throw RuntimeException when looking for non existent cluster`() {
        assertThatThrownBy { cache.getFlinkCluster(ResourceSelector(namespace = "flink", name = "test", uid = "123")) }.isInstanceOf(RuntimeException::class.java)
    }

    @Test
    fun `should return null when looking for non existent cluster id`() {
        assertThat(cache.findClusterSelector(namespace = "flink", name = "test")).isNull()
    }

    @Test
    fun `should return empty list of clusters initially`() {
        assertThat(cache.getFlinkClusters()).isEmpty()
    }

    @Test
    fun `should return empty list of cluster selectors initially`() {
        assertThat(cache.getClusterSelectors()).isEmpty()
    }

    @Test
    fun `should update clusters when a cluster changed`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        cache.updateSnapshot()
        assertThat(cache.getFlinkClusters()).isNotEmpty()
    }

    @Test
    fun `should find cluster when a cluster changed`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        cache.updateSnapshot()
        assertThat(cache.getFlinkCluster(ResourceSelector(namespace = "flink", name = "test", uid = "123"))).isNotNull()
    }

    @Test
    fun `should find cluster selectors when a cluster changed`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        cache.updateSnapshot()
        assertThat(cache.findClusterSelector(namespace = "flink", name = "test")).isNotNull()
    }

    @Test
    fun `should return non empty list of clusters when a cluster changed`() {
        val cluster1 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        cache.updateSnapshot()
        val clusters = cache.getFlinkClusters()
        assertThat(clusters).hasSize(2)
        assertThat(clusters).containsExactlyInAnyOrder(cluster1, cluster2)
    }

    @Test
    fun `should update clusters when a cluster is deleted`() {
        val cluster1 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        cache.onFlinkClusterDeleted(cluster1)
        cache.updateSnapshot()
        val clusters = cache.getFlinkClusters()
        assertThat(clusters).hasSize(1)
        assertThat(clusters).containsExactlyInAnyOrder(cluster2)
    }

    @Test
    fun `should remove all clusters when all clusters are deleted`() {
        val cluster1 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        cache.updateSnapshot()
        assertThat(cache.getFlinkClusters()).hasSize(2)
        cache.onFlinkClusterDeletedAll()
        cache.updateSnapshot()
        assertThat(cache.getFlinkClusters()).hasSize(0)
    }

    @Test
    fun `should return non empty list of cluster ids when a cluster changed`() {
        val cluster1 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        cache.updateSnapshot()
        val clusterSelectors = cache.getClusterSelectors()
        assertThat(clusterSelectors).hasSize(2)
        val clusterSelector1 = ResourceSelector(namespace = "flink", name = "test", uid = "123")
        val clusterSelector2 = ResourceSelector(namespace = "flink", name = "test", uid = "456")
        assertThat(clusterSelectors).containsExactlyInAnyOrder(clusterSelector1, clusterSelector2)
    }

    @Test
    fun `should throw IllegalStateException when cluster is missing name`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cluster.metadata.name = null
        assertThatThrownBy { cache.onFlinkClusterChanged(cluster) }.isInstanceOf(RuntimeException::class.java)
    }

    @Test
    fun `should throw IllegalStateException when cluster is missing uid`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = null
        assertThatThrownBy { cache.onFlinkClusterChanged(cluster) }.isInstanceOf(RuntimeException::class.java)
    }

    @Test
    fun `should return empty resources initially`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        cache.updateSnapshot()
        val clusterSelector = ResourceSelector(namespace = "flink", name = "test", uid = "123")
        val clusterResources = cache.getCachedClusterResources(clusterSelector = clusterSelector)
        assertThat(clusterResources.flinkJobs).isEmpty()
        assertThat(clusterResources.service).isNull()
        assertThat(clusterResources.jobmanagerPods).isEmpty()
        assertThat(clusterResources.taskmanagerPods).isEmpty()
        val jobResources = cache.getCachedJobResources(clusterSelector = clusterSelector, jobName = "test")
        assertThat(jobResources.flinkJob).isNull()
        assertThat(jobResources.bootstrapJob).isNull()
    }

    @Test
    fun `should return resources when resources are changed`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val flinkJob = TestFactory.aFlinkJob(cluster)
        val job = TestFactory.aBootstrapJob(cluster)
        val service = TestFactory.aJobManagerService(cluster)
        val jobManagerPod = TestFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod = TestFactory.aTaskManagerPod(cluster,"1")
        cache.onFlinkJobChanged(flinkJob)
        cache.onJobChanged(job)
        cache.onServiceChanged(service)
        cache.onPodChanged(jobManagerPod)
        cache.onPodChanged(taskManagerPod)
        cache.updateSnapshot()
        val clusterSelector = ResourceSelector(namespace = "flink", name = "test", uid = "123")
        val clusterResources = cache.getCachedClusterResources(clusterSelector = clusterSelector)
        assertThat(clusterResources.flinkJobs).isEqualTo(mapOf("test" to flinkJob))
        assertThat(clusterResources.service).isEqualTo(service)
        assertThat(clusterResources.jobmanagerPods).isEqualTo(setOf(jobManagerPod))
        assertThat(clusterResources.taskmanagerPods).isEqualTo(setOf(taskManagerPod))
        val jobResources = cache.getCachedJobResources(clusterSelector = clusterSelector, jobName = "test")
        assertThat(jobResources.flinkJob).isEqualTo(flinkJob)
        assertThat(jobResources.bootstrapJob).isEqualTo(job)
    }

    @Test
    fun `should update resources when resources are changed`() {
        val cluster1 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val flinkJob1 = TestFactory.aFlinkJob(cluster1)
        val job1 = TestFactory.aBootstrapJob(cluster1)
        val service1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerPod1 = TestFactory.aJobManagerPod(cluster1,"1")
        val taskManagerPod1 = TestFactory.aTaskManagerPod(cluster1,"1")
        val cluster2 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val flinkJob2 = TestFactory.aFlinkJob(cluster2)
        val job2 = TestFactory.aBootstrapJob(cluster2)
        val service2 = TestFactory.aJobManagerService(cluster2)
        val jobManagerPod2 = TestFactory.aJobManagerPod(cluster2,"1")
        val taskManagerPod2 = TestFactory.aTaskManagerPod(cluster2,"1")
        cache.onFlinkJobChanged(flinkJob1)
        cache.onJobChanged(job1)
        cache.onServiceChanged(service1)
        cache.onPodChanged(jobManagerPod1)
        cache.onPodChanged(taskManagerPod1)
        cache.onFlinkJobChanged(flinkJob2)
        cache.onServiceChanged(service2)
        cache.onJobChanged(job2)
        cache.onPodChanged(jobManagerPod2)
        cache.onPodChanged(taskManagerPod2)
        cache.updateSnapshot()
        val clusterSelector1 = ResourceSelector(namespace = "flink", name = "test", uid = "123")
        val clusterResources1 = cache.getCachedClusterResources(clusterSelector = clusterSelector1)
        assertThat(clusterResources1.flinkJobs).isEqualTo(mapOf("test" to flinkJob1))
        assertThat(clusterResources1.service).isEqualTo(service1)
        assertThat(clusterResources1.jobmanagerPods).isEqualTo(setOf(jobManagerPod1))
        assertThat(clusterResources1.taskmanagerPods).isEqualTo(setOf(taskManagerPod1))
        val jobResources1 = cache.getCachedJobResources(clusterSelector = clusterSelector1, jobName = "test")
        assertThat(jobResources1.flinkJob).isEqualTo(flinkJob1)
        assertThat(jobResources1.bootstrapJob).isEqualTo(job1)
        val clusterSelector2 = ResourceSelector(namespace = "flink", name = "test", uid = "456")
        val clusterResources2 = cache.getCachedClusterResources(clusterSelector = clusterSelector2)
        assertThat(clusterResources2.flinkJobs).isEqualTo(mapOf("test" to flinkJob2))
        assertThat(clusterResources2.service).isEqualTo(service2)
        assertThat(clusterResources2.jobmanagerPods).isEqualTo(setOf(jobManagerPod2))
        assertThat(clusterResources2.taskmanagerPods).isEqualTo(setOf(taskManagerPod2))
        val jobResources2 = cache.getCachedJobResources(clusterSelector = clusterSelector2, jobName = "test")
        assertThat(jobResources2.flinkJob).isEqualTo(flinkJob2)
        assertThat(jobResources2.bootstrapJob).isEqualTo(job2)
    }

    @Test
    fun `should update resources when resources are deleted`() {
        val cluster1 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val flinkJob1 = TestFactory.aFlinkJob(cluster1)
        val job1 = TestFactory.aBootstrapJob(cluster1)
        val service1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerPod1 = TestFactory.aJobManagerPod(cluster1,"1")
        val taskManagerPod1 = TestFactory.aTaskManagerPod(cluster1,"1")
        val cluster2 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val flinkJob2 = TestFactory.aFlinkJob(cluster2)
        val job2 = TestFactory.aBootstrapJob(cluster2)
        val service2 = TestFactory.aJobManagerService(cluster2)
        val jobManagerPod2 = TestFactory.aJobManagerPod(cluster2,"1")
        val taskManagerPod2 = TestFactory.aTaskManagerPod(cluster2,"1")
        cache.onFlinkJobChanged(flinkJob1)
        cache.onJobChanged(job1)
        cache.onServiceChanged(service1)
        cache.onPodChanged(jobManagerPod1)
        cache.onPodChanged(taskManagerPod1)
        cache.onFlinkJobChanged(flinkJob2)
        cache.onJobChanged(job2)
        cache.onServiceChanged(service2)
        cache.onPodChanged(jobManagerPod2)
        cache.onPodChanged(taskManagerPod2)
        cache.onFlinkJobDeleted(flinkJob1)
        cache.onJobDeleted(job1)
        cache.onServiceDeleted(service1)
        cache.onPodDeleted(jobManagerPod1)
        cache.onPodDeleted(taskManagerPod1)
        cache.updateSnapshot()
        val clusterSelector1 = ResourceSelector(namespace = "flink", name = "test", uid = "123")
        val clusterResources1 = cache.getCachedClusterResources(clusterSelector = clusterSelector1)
        assertThat(clusterResources1.flinkJobs).isEmpty()
        assertThat(clusterResources1.service).isNull()
        assertThat(clusterResources1.jobmanagerPods).isEmpty()
        assertThat(clusterResources1.taskmanagerPods).isEmpty()
        val jobResources1 = cache.getCachedJobResources(clusterSelector = clusterSelector1, jobName = "test")
        assertThat(jobResources1.flinkJob).isNull()
        assertThat(jobResources1.bootstrapJob).isNull()
        val clusterSelector2 = ResourceSelector(namespace = "flink", name = "test", uid = "456")
        val clusterResources2 = cache.getCachedClusterResources(clusterSelector = clusterSelector2)
        assertThat(clusterResources2.flinkJobs).isEqualTo(mapOf("test" to flinkJob2))
        assertThat(clusterResources2.service).isEqualTo(service2)
        assertThat(clusterResources2.jobmanagerPods).isEqualTo(setOf(jobManagerPod2))
        assertThat(clusterResources2.taskmanagerPods).isEqualTo(setOf(taskManagerPod2))
        val jobResources2 = cache.getCachedJobResources(clusterSelector = clusterSelector2, jobName = "test")
        assertThat(jobResources2.flinkJob).isEqualTo(flinkJob2)
        assertThat(jobResources2.bootstrapJob).isEqualTo(job2)
    }

    @Test
    fun `should update resources when all resources are deleted`() {
        val cluster1 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val flinkJob1 = TestFactory.aFlinkJob(cluster1)
        val job1 = TestFactory.aBootstrapJob(cluster1)
        val service1 = TestFactory.aJobManagerService(cluster1)
        val jobManagerPod1 = TestFactory.aJobManagerPod(cluster1,"1")
        val taskManagerPod1 = TestFactory.aTaskManagerPod(cluster1,"1")
        val cluster2 = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster2.metadata.uid = "456"
        val flinkJob2 = TestFactory.aFlinkJob(cluster2)
        val job2 = TestFactory.aBootstrapJob(cluster2)
        val service2 = TestFactory.aJobManagerService(cluster2)
        val jobManagerPod2 = TestFactory.aJobManagerPod(cluster2,"1")
        val taskManagerPod2 = TestFactory.aTaskManagerPod(cluster2,"1")
        cache.onFlinkJobChanged(flinkJob1)
        cache.onJobChanged(job1)
        cache.onServiceChanged(service1)
        cache.onPodChanged(jobManagerPod1)
        cache.onPodChanged(taskManagerPod1)
        cache.onFlinkJobChanged(flinkJob2)
        cache.onJobChanged(job2)
        cache.onServiceChanged(service2)
        cache.onPodChanged(jobManagerPod2)
        cache.onPodChanged(taskManagerPod2)
        cache.onJobDeletedAll()
        cache.onServiceDeletedAll()
        cache.onPodDeletedAll()
        cache.onFlinkJobsDeletedAll()
        cache.updateSnapshot()
        val clusterSelector1 = ResourceSelector(namespace = "flink", name = "test", uid = "123")
        val clusterResources1 = cache.getCachedClusterResources(clusterSelector = clusterSelector1)
        assertThat(clusterResources1.flinkJobs).isEmpty()
        assertThat(clusterResources1.service).isNull()
        assertThat(clusterResources1.jobmanagerPods).isEmpty()
        assertThat(clusterResources1.taskmanagerPods).isEmpty()
        val jobResources1 = cache.getCachedJobResources(clusterSelector = clusterSelector1, jobName = "test")
        assertThat(jobResources1.flinkJob).isNull()
        assertThat(jobResources1.bootstrapJob).isNull()
        val clusterSelector2 = ResourceSelector(namespace = "flink", name = "test", uid = "456")
        val clusterResources2 = cache.getCachedClusterResources(clusterSelector = clusterSelector2)
        assertThat(clusterResources2.flinkJobs).isEmpty()
        assertThat(clusterResources2.service).isNull()
        assertThat(clusterResources2.jobmanagerPods).isEmpty()
        assertThat(clusterResources2.taskmanagerPods).isEmpty()
        val jobResources2 = cache.getCachedJobResources(clusterSelector = clusterSelector2, jobName = "test")
        assertThat(jobResources2.flinkJob).isNull()
        assertThat(jobResources2.bootstrapJob).isNull()
    }
}
