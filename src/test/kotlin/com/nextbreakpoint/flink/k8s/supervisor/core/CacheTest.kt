package com.nextbreakpoint.flink.k8s.supervisor.core

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
    fun `should return null when looking for non existent cluster`() {
        assertThat(cache.getFlinkCluster("test")).isNull()
    }

    @Test
    fun `should return empty list of clusters initially`() {
        assertThat(cache.getFlinkClusters()).isEmpty()
    }

    @Test
    fun `should return empty list of cluster selectors initially`() {
        assertThat(cache.listClusterNames()).isEmpty()
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
        assertThat(cache.getFlinkCluster("test")).isNotNull()
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
        assertThat(clusters).hasSize(1)
        assertThat(clusters).containsExactlyInAnyOrder(cluster2)
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
        assertThat(cache.getFlinkClusters()).hasSize(0)
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
        assertThat(cache.getFlinkClusters()).hasSize(1)
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
        val clusterSelectors = cache.listClusterNames()
        assertThat(clusterSelectors).hasSize(1)
        assertThat(clusterSelectors).containsExactlyInAnyOrder("test")
    }

    @Test
    fun `should throw IllegalStateException when cluster is missing name`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cluster.metadata.name = null
        assertThatThrownBy { cache.onFlinkClusterChanged(cluster) }.isInstanceOf(RuntimeException::class.java)
    }

    @Test
    fun `should return empty resources initially`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        cache.updateSnapshot()
        val clusterResources = cache.getClusterResources()
        assertThat(clusterResources.flinkJobs).isEmpty()
        assertThat(clusterResources.jobmanagerService).isNull()
        assertThat(clusterResources.jobmanagerPods).isEmpty()
        assertThat(clusterResources.taskmanagerPods).isEmpty()
        val jobResources = cache.getJobResources("test")
        assertThat(jobResources.flinkJob).isNull()
        assertThat(jobResources.bootstrapJob).isNull()
    }

    @Test
    fun `should return resources when resources are changed`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val flinkJob = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")
        val job = TestFactory.aBootstrapJob(cluster, flinkJob)
        val service = TestFactory.aJobManagerService(cluster)
        val jobManagerPod = TestFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod = TestFactory.aTaskManagerPod(cluster,"1")
        cache.onFlinkJobChanged(flinkJob)
        cache.onJobChanged(job)
        cache.onServiceChanged(service)
        cache.onPodChanged(jobManagerPod)
        cache.onPodChanged(taskManagerPod)
        cache.updateSnapshot()
        val clusterResources = cache.getClusterResources()
        assertThat(clusterResources.flinkJobs).isEqualTo(setOf(flinkJob))
        assertThat(clusterResources.jobmanagerService).isEqualTo(service)
        assertThat(clusterResources.jobmanagerPods).isEqualTo(setOf(jobManagerPod))
        assertThat(clusterResources.taskmanagerPods).isEqualTo(setOf(taskManagerPod))
        val jobResources = cache.getJobResources("test")
        assertThat(jobResources.flinkJob).isEqualTo(flinkJob)
        assertThat(jobResources.bootstrapJob).isEqualTo(job)
    }

    @Test
    fun `should update resources when resources are changed`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val flinkJob1 = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")
        val job1 = TestFactory.aBootstrapJob(cluster, flinkJob1)
        val service1 = TestFactory.aJobManagerService(cluster)
        val jobManagerPod1 = TestFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod1 = TestFactory.aTaskManagerPod(cluster,"1")
        val flinkJob2 = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")
        val job2 = TestFactory.aBootstrapJob(cluster, flinkJob2)
        val service2 = TestFactory.aJobManagerService(cluster)
        val jobManagerPod2 = TestFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod2 = TestFactory.aTaskManagerPod(cluster,"1")
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
        val clusterResources = cache.getClusterResources()
        assertThat(clusterResources.flinkJobs).isEqualTo(setOf(flinkJob2))
        assertThat(clusterResources.jobmanagerService).isEqualTo(service2)
        assertThat(clusterResources.jobmanagerPods).isEqualTo(setOf(jobManagerPod2))
        assertThat(clusterResources.taskmanagerPods).isEqualTo(setOf(taskManagerPod2))
        val jobResources = cache.getJobResources("test")
        assertThat(jobResources.flinkJob).isEqualTo(flinkJob2)
        assertThat(jobResources.bootstrapJob).isEqualTo(job2)
    }

    @Test
    fun `should update resources when resources are deleted`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val flinkJob1 = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")
        val job1 = TestFactory.aBootstrapJob(cluster, flinkJob1)
        val service1 = TestFactory.aJobManagerService(cluster)
        val jobManagerPod1 = TestFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod1 = TestFactory.aTaskManagerPod(cluster,"1")
        val flinkJob2 = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")
        val job2 = TestFactory.aBootstrapJob(cluster, flinkJob2)
        val service2 = TestFactory.aJobManagerService(cluster)
        val jobManagerPod2 = TestFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod2 = TestFactory.aTaskManagerPod(cluster,"1")
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
        val clusterResources = cache.getClusterResources()
        assertThat(clusterResources.flinkJobs).isEmpty()
        assertThat(clusterResources.jobmanagerService).isNull()
        assertThat(clusterResources.jobmanagerPods).isEmpty()
        assertThat(clusterResources.taskmanagerPods).isEmpty()
        val jobResources = cache.getJobResources("test")
        assertThat(jobResources.flinkJob).isNull()
        assertThat(jobResources.bootstrapJob).isNull()
    }

    @Test
    fun `should update resources when all resources are deleted`() {
        val cluster = TestFactory.aFlinkCluster(name = "test", namespace = "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val flinkJob1 = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")
        val job1 = TestFactory.aBootstrapJob(cluster, flinkJob1)
        val service1 = TestFactory.aJobManagerService(cluster)
        val jobManagerPod1 = TestFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod1 = TestFactory.aTaskManagerPod(cluster,"1")
        val flinkJob2 = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")
        val job2 = TestFactory.aBootstrapJob(cluster, flinkJob2)
        val service2 = TestFactory.aJobManagerService(cluster)
        val jobManagerPod2 = TestFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod2 = TestFactory.aTaskManagerPod(cluster,"1")
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
        cache.onFlinkJobDeletedAll()
        cache.updateSnapshot()
        val clusterResources = cache.getClusterResources()
        assertThat(clusterResources.flinkJobs).isEmpty()
        assertThat(clusterResources.jobmanagerService).isNull()
        assertThat(clusterResources.jobmanagerPods).isEmpty()
        assertThat(clusterResources.taskmanagerPods).isEmpty()
        val jobResources = cache.getJobResources("test")
        assertThat(jobResources.flinkJob).isNull()
        assertThat(jobResources.bootstrapJob).isNull()
    }
}
