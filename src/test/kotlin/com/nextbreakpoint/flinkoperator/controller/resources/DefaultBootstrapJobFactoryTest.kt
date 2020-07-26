package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class DefaultBootstrapJobFactoryTest {
    private val cluster = TestFactory.aCluster(name = "test", namespace ="flink", taskManagers = 3, taskSlots = 2)

    @Test
    fun `should create bootstrap job when submitting job`() {
        val job = DefaultBootstrapJobFactory.createBootstrapJob(ClusterSelector(namespace = "flink", name = "test", uuid = "xxx"),"myself", cluster.spec.bootstrap, "tmp/001", 4)

        assertThat(job).isNotNull()

        assertThat(job.metadata?.name).isEqualTo("flink-bootstrap-${cluster.metadata.name}")

        val labels = job.metadata?.labels
        assertThat(labels).hasSize(4)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("name")).isEqualTo(cluster.metadata.name)
        assertThat(labels?.get("uid")).isEqualTo("xxx")
        assertThat(labels?.get("component")).isEqualTo("flink")

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
        assertThat(container?.args).hasSize(10)
        assertThat(container?.args?.get(0)).isEqualTo("bootstrap")
        assertThat(container?.args?.get(1)).isEqualTo("run")
        assertThat(container?.args?.get(2)).isEqualTo("--namespace=flink")
        assertThat(container?.args?.get(3)).isEqualTo("--cluster-name=${cluster.metadata.name}")
        assertThat(container?.args?.get(4)).isEqualTo("--jar-path=${cluster.spec?.bootstrap?.jarPath}")
        assertThat(container?.args?.get(5)).isEqualTo("--class-name=${cluster.spec?.bootstrap?.className}")
        assertThat(container?.args?.get(6)).isEqualTo("--parallelism=4")
        assertThat(container?.args?.get(7)).isEqualTo("--savepoint-path=tmp/001")
        assertThat(container?.args?.get(8)).isEqualTo("--argument=--BUCKET_BASE_PATH")
        assertThat(container?.args?.get(9)).isEqualTo("--argument=file:///var/tmp")
        assertThat(container?.env).hasSize(2)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
    }
}