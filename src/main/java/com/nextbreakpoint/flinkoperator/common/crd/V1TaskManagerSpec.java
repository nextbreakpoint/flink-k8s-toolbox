package com.nextbreakpoint.flinkoperator.common.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerPort;
import io.kubernetes.client.models.V1EnvFromSource;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    public Integer getTaskSlots() {
        return taskSlots;
    }

    public V1TaskManagerSpec setTaskSlots(Integer taskSlots) {
        this.taskSlots = taskSlots;
        return this;
    }

    public List<V1EnvVar> getEnvironment() {
        return environment;
    }

    public V1TaskManagerSpec setEnvironment(List<V1EnvVar> environment) {
        this.environment = environment;
        return this;
    }

    public List<V1EnvFromSource> getEnvironmentFrom() {
        return environmentFrom;
    }

    public void setEnvironmentFrom(List<V1EnvFromSource> environmentFrom) {
        this.environmentFrom = environmentFrom;
    }

    public String getServiceAccount() {
        return serviceAccount;
    }

    public V1TaskManagerSpec setServiceAccount(String serviceAccount) {
        this.serviceAccount = serviceAccount;
        return this;
    }

    public List<V1Volume> getVolumes() {
        return volumes;
    }

    public V1TaskManagerSpec setVolumes(List<V1Volume> volumes) {
        this.volumes = volumes;
        return this;
    }

    public List<V1VolumeMount> getVolumeMounts() {
        return volumeMounts;
    }

    public V1TaskManagerSpec setVolumeMounts(List<V1VolumeMount> volumeMounts) {
        this.volumeMounts = volumeMounts;
        return this;
    }

    public Map<String, String> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(Map<String, String> annotations) {
        this.annotations = annotations;
    }

    public List<V1ContainerPort> getExtraPorts() {
        return extraPorts;
    }

    public void setExtraPorts(List<V1ContainerPort> extraPorts) {
        this.extraPorts = extraPorts;
    }

    public List<V1Container> getInitContainers() {
        return initContainers;
    }

    public void setInitContainers(List<V1Container> initContainers) {
        this.initContainers = initContainers;
    }

    public List<V1Container> getSideContainers() {
        return sideContainers;
    }

    public void setSideContainers(List<V1Container> sideContainers) {
        this.sideContainers = sideContainers;
    }

    public V1ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(V1ResourceRequirements resources) {
        this.resources = resources;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1TaskManagerSpec that = (V1TaskManagerSpec) o;
        return Objects.equals(getTaskSlots(), that.getTaskSlots()) &&
                Objects.equals(getEnvironment(), that.getEnvironment()) &&
                Objects.equals(getEnvironmentFrom(), that.getEnvironmentFrom()) &&
                Objects.equals(getServiceAccount(), that.getServiceAccount()) &&
                Objects.equals(getVolumes(), that.getVolumes()) &&
                Objects.equals(getVolumeMounts(), that.getVolumeMounts()) &&
                Objects.equals(getAnnotations(), that.getAnnotations()) &&
                Objects.equals(getExtraPorts(), that.getExtraPorts()) &&
                Objects.equals(getInitContainers(), that.getInitContainers()) &&
                Objects.equals(getSideContainers(), that.getSideContainers()) &&
                Objects.equals(getResources(), that.getResources());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTaskSlots(), getEnvironment(), getEnvironmentFrom(), getServiceAccount(), getVolumes(), getVolumeMounts(), getAnnotations(), getExtraPorts(), getInitContainers(), getSideContainers(), getResources());
    }

    @Override
    public String toString() {
        return "V1TaskManagerSpec{" +
                "taskSlots=" + taskSlots +
                ", environment=" + environment +
                ", environmentFrom=" + environmentFrom +
                ", serviceAccount='" + serviceAccount + '\'' +
                ", volumes=" + volumes +
                ", volumeMounts=" + volumeMounts +
                ", annotations=" + annotations +
                ", extraPorts=" + extraPorts +
                ", initContainers=" + initContainers +
                ", sideContainers=" + sideContainers +
                ", resources=" + resources +
                '}';
    }
}
