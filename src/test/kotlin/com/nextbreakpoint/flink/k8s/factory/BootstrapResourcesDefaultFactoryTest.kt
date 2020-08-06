package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class BootstrapResourcesDefaultFactoryTest {
    private val flinkCluster = TestFactory.aFlinkCluster(name = "test", namespace ="flink", taskManagers = 3, taskSlots = 2)
    private val flinkJob = TestFactory.aFlinkJob(flinkCluster)
    private val clusterSelector = ResourceSelector(namespace = "flink", name = "test", uid = "xxx")
    private val jobSelector = ResourceSelector(namespace = "flink", name = "test-test", uid = "xxx")

    @Test
    fun `should create bootstrap job when submitting job`() {
        val job = BootstrapResourcesDefaultFactory.createBootstrapJob(clusterSelector, jobSelector, "myself", "test", flinkJob.spec.bootstrap, "tmp/001", 4, false)

        assertThat(job).isNotNull()

        assertThat(job.metadata?.generateName).isEqualTo("bootstrap-${flinkJob.metadata.name}-")

        val labels = job.metadata?.labels
        assertThat(labels).hasSize(6)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("clusterName")).isEqualTo(flinkCluster.metadata.name)
        assertThat(labels?.get("clusterUid")).isEqualTo("xxx")
        assertThat(labels?.get("jobName")).isEqualTo("test")
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("job")).isEqualTo("bootstrap")

        assertThat(job.spec?.parallelism).isEqualTo(1)
        assertThat(job.spec?.completions).isEqualTo(1)
        assertThat(job.spec?.backoffLimit).isEqualTo(1)
        assertThat(job.spec?.ttlSecondsAfterFinished).isEqualTo(30)

        val podSpec = job.spec?.template?.spec
        assertThat(podSpec?.restartPolicy).isEqualTo("OnFailure")
        assertThat(podSpec?.serviceAccountName).isEqualTo("bootstrap-test")
        assertThat(podSpec?.imagePullSecrets).hasSize(1)
        assertThat(podSpec?.imagePullSecrets?.get(0)?.name).isEqualTo("bootstrap-regcred")
        assertThat(podSpec?.affinity).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity?.preferredDuringSchedulingIgnoredDuringExecution).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity?.preferredDuringSchedulingIgnoredDuringExecution).hasSize(1)
        assertThat(podSpec?.containers).hasSize(1)

        val container = podSpec?.containers?.get(0)
        assertThat(container?.ports).isNull()
        assertThat(container?.imagePullPolicy).isEqualTo("IfNotPresent")
        assertThat(container?.args).hasSize(11)
        assertThat(container?.args?.get(0)).isEqualTo("bootstrap")
        assertThat(container?.args?.get(1)).isEqualTo("run")
        assertThat(container?.args?.get(2)).isEqualTo("--namespace=flink")
        assertThat(container?.args?.get(3)).isEqualTo("--cluster-name=${flinkCluster.metadata.name}")
        assertThat(container?.args?.get(4)).isEqualTo("--job-name=test-test")
        assertThat(container?.args?.get(5)).isEqualTo("--jar-path=${flinkJob.spec?.bootstrap?.jarPath}")
        assertThat(container?.args?.get(6)).isEqualTo("--class-name=${flinkJob.spec?.bootstrap?.className}")
        assertThat(container?.args?.get(7)).isEqualTo("--parallelism=4")
        assertThat(container?.args?.get(8)).isEqualTo("--savepoint-path=tmp/001")
        assertThat(container?.args?.get(9)).isEqualTo("--argument=--BUCKET_BASE_PATH")
        assertThat(container?.args?.get(10)).isEqualTo("--argument=file:///var/tmp")
        assertThat(container?.env).hasSize(2)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
    }
}