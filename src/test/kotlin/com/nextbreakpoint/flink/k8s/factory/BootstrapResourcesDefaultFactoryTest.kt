package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class BootstrapResourcesDefaultFactoryTest {
    private val flinkCluster = TestFactory.aFlinkCluster(name = "test", namespace ="flink", taskManagers = 3, taskSlots = 2)
    private val flinkJob = TestFactory.aFlinkJob(name = "test-test", namespace = "flink")

    @Test
    fun `should create bootstrap job when submitting job`() {
        val job = BootstrapResourcesDefaultFactory.createBootstrapJob("flink", "myself", "test", "test", flinkJob.spec.bootstrap, "tmp/001", 4, false)

        assertThat(job).isNotNull()

        assertThat(job.metadata?.name).isEqualTo("bootstrap-${flinkJob.metadata.name}")

        val labels = job.metadata?.labels
        assertThat(labels).hasSize(5)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("clusterName")).isEqualTo("test")
        assertThat(labels?.get("jobName")).isEqualTo("test")
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("bootstrap")

        assertThat(job.spec?.parallelism).isEqualTo(1)
        assertThat(job.spec?.completions).isEqualTo(1)
        assertThat(job.spec?.backoffLimit).isEqualTo(1)
        assertThat(job.spec?.ttlSecondsAfterFinished).isEqualTo(30)

        assertThat(job.spec?.template?.metadata?.name).isEqualTo("bootstrap-${flinkJob.metadata.name}")

        val templateLabels = job.spec?.template?.metadata?.labels
        assertThat(templateLabels).hasSize(5)
        assertThat(templateLabels?.get("owner")).isEqualTo("myself")
        assertThat(templateLabels?.get("clusterName")).isEqualTo("test")
        assertThat(templateLabels?.get("jobName")).isEqualTo("test")
        assertThat(templateLabels?.get("component")).isEqualTo("flink")
        assertThat(templateLabels?.get("role")).isEqualTo("bootstrap")

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
        assertThat(container?.args?.get(3)).isEqualTo("--cluster-name=test")
        assertThat(container?.args?.get(4)).isEqualTo("--job-name=test")
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