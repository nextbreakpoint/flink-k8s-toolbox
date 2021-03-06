package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1SecurityContext;
import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true, setterPrefix = "with")
public class V1SupervisorSpec {
    @SerializedName("pullSecrets")
    private String pullSecrets;
    @SerializedName("pullPolicy")
    private String pullPolicy;
    @SerializedName("image")
    private String image;
    @SerializedName("serviceAccount")
    private String serviceAccount;
    @SerializedName("pollingInterval")
    private String pollingInterval;
    @SerializedName("taskTimeout")
    private Integer taskTimeout;
    @SerializedName("maxTaskManagers")
    private Integer maxTaskManagers;
    @SerializedName("resources")
    private V1ResourceRequirements resources;
    @SerializedName("rescaleDelay")
    private Integer rescaleDelay;
    @SerializedName("rescalePolicy")
    private String rescalePolicy;
    @SerializedName("replicas")
    private Integer replicas;
    @SerializedName("securityContext")
    private V1SecurityContext securityContext;
}
