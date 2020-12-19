package com.nextbreakpoint.flink.k8s.supervisor

import com.nextbreakpoint.flink.common.RunnerOptions
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.max

class SupervisorRunner(
    private val registry: MeterRegistry,
    private val controller: Controller,
    private val cache: Cache,
    private val options: RunnerOptions
) {
    companion object {
        private val logger = Logger.getLogger(SupervisorRunner::class.simpleName)
    }

    fun run() {
        val supervisor = Supervisor.create(controller, cache, options.taskTimeout, options.pollingInterval, options.serverConfig)

        while (!Thread.interrupted()) {
            TimeUnit.SECONDS.sleep(max(options.pollingInterval, 5))

            try {
                val currentTimeMillis = controller.currentTimeMillis()
                if (currentTimeMillis - cache.getLastResetTimestamp() < 10000) {
                    logger.log(Level.WARNING, "Cache has been cleared less than 10 seconds ago")
                    continue
                }
                if (currentTimeMillis - cache.getLastUpdateTimestamp() < 4500) {
                    logger.log(Level.INFO, "Cache has been modified less than 5 seconds ago")
                    continue
                }
                cache.takeSnapshot()
                supervisor.reconcile()
                supervisor.cleanup()
            } catch (e: Exception) {
                logger.log(Level.SEVERE, "Something went wrong", e)
            }
        }
    }
}