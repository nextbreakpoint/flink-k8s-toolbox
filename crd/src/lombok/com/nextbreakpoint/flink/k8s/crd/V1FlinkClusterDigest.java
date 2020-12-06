package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkClusterDigest {
    @SerializedName("runtime")
    private String runtime;
    @SerializedName("jobManager")
    private String jobManager;
    @SerializedName("taskManager")
    private String taskManager;
    @SerializedName("supervisor")
    private String supervisor;
}
