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
public class V1FlinkDeploymentJobSpec {
    @SerializedName("name")
    private String name;
    @SerializedName("spec")
    private V1FlinkJobSpec spec;
}