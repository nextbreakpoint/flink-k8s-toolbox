package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkDeploymentDigest {
    @SerializedName("cluster")
    private V1FlinkClusterDigest cluster;
    @SerializedName("jobs")
    private List<V1FlinkDeploymentJobDigest> jobs;
}
