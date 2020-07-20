package com.nextbreakpoint.flinkoperator.integration.cases

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flinkoperator.integration.IntegrationSetup
import io.kubernetes.client.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files

@Tag("IntegrationTest")
class CreateAndDeleteTest : IntegrationSetup() {
    companion object {
        @BeforeAll
        @JvmStatic
        fun setup() {
            IntegrationSetup.setup()
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            println("Removing finalizers...")
            removeFinalizers(name = "cluster-0")
            deleteClusterByName(redirect = redirect, namespace = namespace, name = "cluster-0")
            awaitUntilAsserted(timeout = 360) {
                assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-0")).isFalse()
            }
            IntegrationSetup.teardown()
        }
    }

    @AfterEach
    fun printInfo() {
        describeResources()
        printOperatorLogs()
    }

    @Test
    fun `should create and delete clusters`() {
        println("Should create cluster...")
        val json = String(Files.readAllBytes(File("integration/cluster-spec.json").toPath()))
        val spec = JSON().deserialize<V1FlinkClusterSpec>(json, specTypeToken.type)
        createCluster(name = "cluster-0", spec = spec, port = port)
        awaitUntilAsserted(timeout = 30) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-0")).isTrue()
        }
        describePods(redirect = redirect, namespace = namespace)
        describeClusters(redirect = redirect, namespace = namespace)
        println("Cluster created")
        println("Should delete cluster...")
        deleteCluster(name = "cluster-0", port = port)
        awaitUntilAsserted(timeout = 300) {
            assertThat(clusterExists(redirect = redirect, namespace = namespace, name = "cluster-0")).isFalse()
        }
        println("Cluster delete")
        describePods(redirect = redirect, namespace = namespace)
        describeClusters(redirect = redirect, namespace = namespace)
    }
}
