package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkDeploymentSpec {
    @SerializedName("cluster")
    private V1FlinkClusterSpec cluster;
    @SerializedName("jobs")
    private List<V1FlinkDeploymentJobSpec> jobs;
}
