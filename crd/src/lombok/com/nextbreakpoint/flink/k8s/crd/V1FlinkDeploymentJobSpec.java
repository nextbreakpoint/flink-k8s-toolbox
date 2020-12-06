package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkDeploymentJobSpec {
    @SerializedName("name")
    private String name;
    @SerializedName("spec")
    private V1FlinkJobSpec spec;
}