package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import io.kubernetes.client.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files

@Tag("IntegrationTest")
class CreateAndDeleteTest : IntegrationSetup() {
    @Test
    fun `should create and delete clusters`() {
        println("Should create cluster...")
        val json = String(Files.readAllBytes(File("example/cluster-spec.json").toPath()))
        val spec = JSON().deserialize<V1FlinkClusterSpec>(json, specTypeToken.type)
        createCluster(name = "cluster-0", spec = spec)
        awaitUntilAsserted(timeout = 60) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-0")).isTrue()
        }
        println("Cluster created")
        println("Should delete cluster...")
        deleteCluster(name = "cluster-0")
        awaitUntilAsserted(timeout = 60) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-0")).isFalse()
        }
        println("Cluster delete")
    }
}