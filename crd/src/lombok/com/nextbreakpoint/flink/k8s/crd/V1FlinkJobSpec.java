package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkJobSpec {
    @SerializedName("bootstrap")
    private V1BootstrapSpec bootstrap;
    @SerializedName("savepoint")
    private V1SavepointSpec savepoint;
    @SerializedName("restart")
    private V1RestartSpec restart;
    @SerializedName("jobParallelism")
    private Integer jobParallelism;
    @SerializedName("minJobParallelism")
    private Integer minJobParallelism;
    @SerializedName("maxJobParallelism")
    private Integer maxJobParallelism;
}
