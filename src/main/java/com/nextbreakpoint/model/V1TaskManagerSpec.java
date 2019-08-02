package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.models.V1EnvFromSource;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1PersistentVolumeClaim;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;

import java.util.List;
import java.util.Objects;

public class V1TaskManagerSpec {
    @SerializedName("requiredCPUs")
    private Float requiredCPUs;
    @SerializedName("requiredMemory")
    private Integer requiredMemory;
    @SerializedName("replicas")
    private Integer replicas;
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
    @SerializedName("persistentVolumeClaimsTemplates")
    private List<V1PersistentVolumeClaim> persistentVolumeClaimsTemplates;

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

    public List<V1PersistentVolumeClaim> getPersistentVolumeClaimsTemplates() {
        return persistentVolumeClaimsTemplates;
    }

    public V1TaskManagerSpec setPersistentVolumeClaimsTemplates(List<V1PersistentVolumeClaim> persistentVolumeClaimsTemplates) {
        this.persistentVolumeClaimsTemplates = persistentVolumeClaimsTemplates;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1TaskManagerSpec that = (V1TaskManagerSpec) o;
        return Objects.equals(getRequiredCPUs(), that.getRequiredCPUs()) &&
                Objects.equals(getRequiredMemory(), that.getRequiredMemory()) &&
                Objects.equals(getReplicas(), that.getReplicas()) &&
                Objects.equals(getTaskSlots(), that.getTaskSlots()) &&
                Objects.equals(getEnvironment(), that.getEnvironment()) &&
                Objects.equals(getEnvironmentFrom(), that.getEnvironmentFrom()) &&
                Objects.equals(getServiceAccount(), that.getServiceAccount()) &&
                Objects.equals(getVolumes(), that.getVolumes()) &&
                Objects.equals(getVolumeMounts(), that.getVolumeMounts()) &&
                Objects.equals(getPersistentVolumeClaimsTemplates(), that.getPersistentVolumeClaimsTemplates());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRequiredCPUs(), getRequiredMemory(), getReplicas(), getTaskSlots(), getEnvironment(), getEnvironmentFrom(), getServiceAccount(), getVolumes(), getVolumeMounts(), getPersistentVolumeClaimsTemplates());
    }

    @Override
    public String toString() {
        return "V1TaskManagerSpec{" +
                "requiredCPUs=" + requiredCPUs +
                ", requiredMemory=" + requiredMemory +
                ", replicas=" + replicas +
                ", taskSlots=" + taskSlots +
                ", environment=" + environment +
                ", environmentFrom=" + environmentFrom +
                ", serviceAccount=" + serviceAccount +
                ", volumes='" + volumes + '\'' +
                ", volumeMounts='" + volumeMounts + '\'' +
                ", persistentVolumeClaimsTemplates='" + persistentVolumeClaimsTemplates + '\'' +
                '}';
    }
}
