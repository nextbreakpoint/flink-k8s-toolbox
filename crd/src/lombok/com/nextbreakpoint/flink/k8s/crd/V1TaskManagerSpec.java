package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvFromSource;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1SecurityContext;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Toleration;
import io.kubernetes.client.openapi.models.V1TopologySpreadConstraint;
import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true, setterPrefix = "with")
public class V1TaskManagerSpec {
    @SerializedName("taskSlots")
    private Integer taskSlots;
    @SerializedName("environment")
    private List<V1EnvVar> environment;
    @SerializedName("environmentFrom")
    private List<V1EnvFromSource> environmentFrom;
    @SerializedName("serviceAccount")
    private String serviceAccount;
    @SerializedName("volumes")
    private List<V1Volume> volumes;
    @SerializedName("volumeMounts")
    private List<V1VolumeMount> volumeMounts;
    @SerializedName("annotations")
    private Map<String, String> annotations;
    @SerializedName("extraPorts")
    private List<V1ContainerPort> extraPorts;
    @SerializedName("initContainers")
    private List<V1Container> initContainers;
    @SerializedName("sideContainers")
    private List<V1Container> sideContainers;
    @SerializedName("resources")
    private V1ResourceRequirements resources;
    @SerializedName("securityContext")
    private V1SecurityContext securityContext;
    @SerializedName("command")
    private List<String> command;
    @SerializedName("args")
    private List<String> args;
    @SerializedName("affinity")
    private V1Affinity affinity;
    @SerializedName("tolerations")
    private List<V1Toleration> tolerations;
    @SerializedName("topologySpreadConstraints")
    private List<V1TopologySpreadConstraint> topologySpreadConstraints;
}
