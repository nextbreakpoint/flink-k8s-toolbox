package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;
import org.joda.time.DateTime;

@Data
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkDeploymentStatus {
    @SerializedName("resourceStatus")
    private String resourceStatus;
    @SerializedName("timestamp")
    private DateTime timestamp;
    @SerializedName("digest")
    private V1FlinkDeploymentDigest digest;
}
