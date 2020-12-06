package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;
import org.joda.time.DateTime;

@Data
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkClusterStatus {
    @SerializedName("resourceStatus")
    private String resourceStatus;
    @SerializedName("supervisorStatus")
    private String supervisorStatus;
    @SerializedName("labelSelector")
    private String labelSelector;
    @SerializedName("clusterHealth")
    private String clusterHealth;
    @SerializedName("taskManagers")
    private Integer taskManagers;
    @SerializedName("taskSlots")
    private Integer taskSlots;
    @SerializedName("taskManagerReplicas")
    private Integer taskManagerReplicas;
    @SerializedName("totalTaskSlots")
    private Integer totalTaskSlots;
    @SerializedName("timestamp")
    private DateTime timestamp;
    @SerializedName("serviceMode")
    private String serviceMode;
    @SerializedName("digest")
    private V1FlinkClusterDigest digest;
    @SerializedName("rescaleTimestamp")
    private DateTime rescaleTimestamp;
}