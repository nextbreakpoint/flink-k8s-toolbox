package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkCluster {
    @SerializedName("apiVersion")
    private String apiVersion;
    @SerializedName("kind")
    private String kind;
    @SerializedName("metadata")
    private V1ObjectMeta metadata;
    @SerializedName("status")
    private V1FlinkClusterStatus status;
    @SerializedName("spec")
    private V1FlinkClusterSpec spec;
}
