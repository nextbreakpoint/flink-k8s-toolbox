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
public class V1FlinkClusterSpec {
    @SerializedName("runtime")
    private V1RuntimeSpec runtime;
    @SerializedName("jobManager")
    private V1JobManagerSpec jobManager;
    @SerializedName("taskManager")
    private V1TaskManagerSpec taskManager;
    @SerializedName("supervisor")
    private V1SupervisorSpec supervisor;
    @SerializedName("taskManagers")
    private Integer taskManagers;
    @SerializedName("minTaskManagers")
    private Integer minTaskManagers;
    @SerializedName("maxTaskManagers")
    private Integer maxTaskManagers;
}
