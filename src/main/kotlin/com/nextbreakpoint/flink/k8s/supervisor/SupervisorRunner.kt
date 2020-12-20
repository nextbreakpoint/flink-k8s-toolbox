package com.nextbreakpoint.flink.k8s.supervisor

import com.nextbreakpoint.flink.common.RunnerOptions
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import com.nextbreakpoint.flink.k8s.supervisor.core.Adapter
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.max
import kotlin.math.min

class SupervisorRunner(
    private val registry: MeterRegistry,
    private val controller: Controller,
    private val cache: Cache,
    private val adapter: Adapter,
    private val options: RunnerOptions
) {
    companion object {
        private val logger = Logger.getLogger(SupervisorRunner::class.simpleName)
    }

    fun run() {
        try {
            val supervisorController = SupervisorController(registry, controller, cache, adapter, options)

            adapter.start(supervisorController)
        } finally {
            adapter.stop();
        }
    }

    private class SupervisorController(
        private val registry: MeterRegistry,
        private val controller: Controller,
        private val cache: Cache,
        private val adapter: Adapter,
        private val options: RunnerOptions
    ) : io.kubernetes.client.extended.controller.Controller {
        @Volatile
        private var running = true

        override fun run() {
            try {
                val supervisor = Supervisor.create(controller, cache, options.taskTimeout, options.pollingInterval, options.serverConfig)

                val pollingInterval = max(min(options.pollingInterval, 60L), 5L)

                while (running) {
                    try {
                        if (adapter.haveSynced()) {
                            cache.takeSnapshot()
                            supervisor.reconcile()
                            supervisor.cleanup()
                        } else {
                            logger.log(Level.INFO, "Cache not synced yet")
                        }

                        TimeUnit.SECONDS.sleep(pollingInterval)
                    } catch (e: Exception) {
                        logger.log(Level.SEVERE, "Something went wrong", e)
                    }
                }
            } catch (e: InterruptedException) {
                // do nothing
            } catch (e: Exception) {
                logger.log(Level.SEVERE, "Something went wrong", e)
            }
        }

        override fun shutdown() {
            running = false
        }
    }
}