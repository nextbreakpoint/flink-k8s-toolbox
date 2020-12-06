package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkJobSpec {
    @SerializedName("bootstrap")
    private V1BootstrapSpec bootstrap;
    @SerializedName("savepoint")
    private V1SavepointSpec savepoint;
    @SerializedName("jobParallelism")
    private Integer jobParallelism;
    @SerializedName("minJobParallelism")
    private Integer minJobParallelism;
    @SerializedName("maxJobParallelism")
    private Integer maxJobParallelism;
}
