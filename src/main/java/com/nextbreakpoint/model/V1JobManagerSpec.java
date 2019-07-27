package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.models.V1EnvVar;

import java.util.List;
import java.util.Objects;

public class V1JobManagerSpec {
    @SerializedName("requiredCPUs")
    private Float requiredCPUs;
    @SerializedName("requiredMemory")
    private Integer requiredMemory;
    @SerializedName("requiredStorageSize")
    private Integer requiredStorageSize;
    @SerializedName("storageClass")
    private String storageClass;
    @SerializedName("environment")
    private List<V1EnvVar> environment;
    @SerializedName("serviceMode")
    private String serviceMode;

    public Float getRequiredCPUs() {
        return requiredCPUs;
    }

    public V1JobManagerSpec setRequiredCPUs(Float requiredCPUs) {
        this.requiredCPUs = requiredCPUs;
        return this;
    }

    public Integer getRequiredMemory() {
        return requiredMemory;
    }

    public V1JobManagerSpec setRequiredMemory(Integer requiredMemory) {
        this.requiredMemory = requiredMemory;
        return this;
    }

    public Integer getRequiredStorageSize() {
        return requiredStorageSize;
    }

    public V1JobManagerSpec setRequiredStorageSize(Integer requiredStorageSize) {
        this.requiredStorageSize = requiredStorageSize;
        return this;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public V1JobManagerSpec setStorageClass(String storageClass) {
        this.storageClass = storageClass;
        return this;
    }

    public List<V1EnvVar> getEnvironment() {
        return environment;
    }

    public V1JobManagerSpec setEnvironment(List<V1EnvVar> environment) {
        this.environment = environment;
        return this;
    }

    public String getServiceMode() {
        return serviceMode;
    }

    public V1JobManagerSpec setServiceMode(String serviceMode) {
        this.serviceMode = serviceMode;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1JobManagerSpec that = (V1JobManagerSpec) o;
        return Objects.equals(getRequiredCPUs(), that.getRequiredCPUs()) &&
                Objects.equals(getRequiredMemory(), that.getRequiredMemory()) &&
                Objects.equals(getRequiredStorageSize(), that.getRequiredStorageSize()) &&
                Objects.equals(getStorageClass(), that.getStorageClass()) &&
                Objects.equals(getEnvironment(), that.getEnvironment()) &&
                Objects.equals(getServiceMode(), that.getServiceMode());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRequiredCPUs(), getRequiredMemory(), getRequiredStorageSize(), getStorageClass(), getEnvironment(), getServiceMode());
    }

    @Override
    public String toString() {
        return "V1JobManagerSpec{" +
                "requiredCPUs=" + requiredCPUs +
                ", requiredMemory=" + requiredMemory +
                ", requiredStorageSize=" + requiredStorageSize +
                ", storageClass='" + storageClass + '\'' +
                ", environment=" + environment +
                ", serviceMode='" + serviceMode + '\'' +
                '}';
    }
}
