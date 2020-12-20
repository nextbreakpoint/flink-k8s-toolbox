package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1SecurityContext;
import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true, setterPrefix = "with")
public class V1BootstrapSpec {
    @SerializedName("pullSecrets")
    private String pullSecrets;
    @SerializedName("pullPolicy")
    private String pullPolicy;
    @SerializedName("image")
    private String image;
    @SerializedName("className")
    private String className;
    @SerializedName("jarPath")
    private String jarPath;
    @SerializedName("arguments")
    private List<String> arguments;
    @SerializedName("serviceAccount")
    private String serviceAccount;
    @SerializedName("resources")
    private V1ResourceRequirements resources;
    @SerializedName("securityContext")
    private V1SecurityContext securityContext;
}
