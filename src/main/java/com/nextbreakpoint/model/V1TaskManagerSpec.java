package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;

import java.util.List;
import java.util.Objects;

public class V1TaskManagerSpec {
    @SerializedName("requiredCPUs")
    private Float requiredCPUs;
    @SerializedName("requiredMemory")
    private Integer requiredMemory;
    @SerializedName("requiredStorageSize")
    private Integer requiredStorageSize;
    @SerializedName("storageClass")
    private String storageClass;
    @SerializedName("replicas")
    private Integer replicas;
    @SerializedName("taskSlots")
    private Integer taskSlots;
    @SerializedName("environment")
    private List<V1EnvVar> environment;
    @SerializedName("serviceAccount")
    private String serviceAccount;
    @SerializedName("volumes")
    private List<V1Volume> volumes;
    @SerializedName("volumeMounts")
    private List<V1VolumeMount> volumeMounts;

    public Float getRequiredCPUs() {
        return requiredCPUs;
    }

    public V1TaskManagerSpec setRequiredCPUs(Float requiredCPUs) {
        this.requiredCPUs = requiredCPUs;
        return this;
    }

    public Integer getRequiredMemory() {
        return requiredMemory;
    }

    public V1TaskManagerSpec setRequiredMemory(Integer requiredMemory) {
        this.requiredMemory = requiredMemory;
        return this;
    }

    public Integer getRequiredStorageSize() {
        return requiredStorageSize;
    }

    public V1TaskManagerSpec setRequiredStorageSize(Integer requiredStorageSize) {
        this.requiredStorageSize = requiredStorageSize;
        return this;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public V1TaskManagerSpec setStorageClass(String storageClass) {
        this.storageClass = storageClass;
        return this;
    }

    public Integer getReplicas() {
        return replicas;
    }

    public V1TaskManagerSpec setReplicas(Integer replicas) {
        this.replicas = replicas;
        return this;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1TaskManagerSpec that = (V1TaskManagerSpec) o;
        return Objects.equals(getRequiredCPUs(), that.getRequiredCPUs()) &&
                Objects.equals(getRequiredMemory(), that.getRequiredMemory()) &&
                Objects.equals(getRequiredStorageSize(), that.getRequiredStorageSize()) &&
                Objects.equals(getStorageClass(), that.getStorageClass()) &&
                Objects.equals(getReplicas(), that.getReplicas()) &&
                Objects.equals(getTaskSlots(), that.getTaskSlots()) &&
                Objects.equals(getEnvironment(), that.getEnvironment()) &&
                Objects.equals(getServiceAccount(), that.getServiceAccount()) &&
                Objects.equals(getVolumes(), that.getVolumes()) &&
                Objects.equals(getVolumeMounts(), that.getVolumeMounts());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRequiredCPUs(), getRequiredMemory(), getRequiredStorageSize(), getStorageClass(), getReplicas(), getTaskSlots(), getEnvironment(), getServiceAccount(), getVolumes(), getVolumeMounts());
    }

    @Override
    public String toString() {
        return "V1TaskManagerSpec{" +
                "requiredCPUs=" + requiredCPUs +
                ", requiredMemory=" + requiredMemory +
                ", requiredStorageSize=" + requiredStorageSize +
                ", storageClass='" + storageClass + '\'' +
                ", replicas=" + replicas +
                ", taskSlots=" + taskSlots +
                ", environment=" + environment +
                ", serviceAccount=" + serviceAccount +
                ", volumes='" + volumes + '\'' +
                ", volumeMounts='" + volumeMounts + '\'' +
                '}';
    }
}
