package com.nextbreakpoint

import org.junit.Test
import java.util.regex.Pattern
import java.util.stream.Collectors
import kotlin.test.assertEquals

class FlinkSubmitTest {
    @Test
    fun `should parse arguments`() {
        val results = Pattern.compile("(--([^ ]+)=(\"[^=]+\"))|(--([^ ]+)=([^\"= ]+))").matcher("--cluster-name=test --class-name=testClass --arguments=\"--BUCKET_BASE_PATH file:///var/tmp/flink --JOB_PARALLELISM 1\"").results().collect(Collectors.toList())
        assertEquals("cluster-name", results.get(0).group(5))
        assertEquals("test", results.get(0).group(6))
        assertEquals("class-name", results.get(1).group(5))
        assertEquals("testClass", results.get(1).group(6))
        assertEquals("arguments", results.get(2).group(2))
        assertEquals("\"--BUCKET_BASE_PATH file:///var/tmp/flink --JOB_PARALLELISM 1\"", results.get(2).group(3))
    }
}
