package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import java.util.Objects;

public class V1FlinkJob {
    @SerializedName("apiVersion")
    private String apiVersion = null;
    @SerializedName("kind")
    private String kind = null;
    @SerializedName("metadata")
    private V1ObjectMeta metadata = null;
    @SerializedName("status")
    private V1FlinkJobStatus status = null;
    @SerializedName("spec")
    private V1FlinkJobSpec spec = null;

    public V1FlinkJob() {
    }

    public V1FlinkJob apiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
        return this;
    }

    public String getApiVersion() {
        return this.apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public V1FlinkJob kind(String kind) {
        this.kind = kind;
        return this;
    }

    public String getKind() {
        return this.kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public V1FlinkJob metadata(V1ObjectMeta metadata) {
        this.metadata = metadata;
        return this;
    }

    public V1ObjectMeta getMetadata() {
        return this.metadata;
    }

    public void setMetadata(V1ObjectMeta metadata) {
        this.metadata = metadata;
    }

    public V1FlinkJobSpec getSpec() {
        return spec;
    }

    public void setSpec(V1FlinkJobSpec spec) {
        this.spec = spec;
    }

    public V1FlinkJob spec(V1FlinkJobSpec spec) {
        this.spec = spec;
        return this;
    }

    public V1FlinkJobStatus getStatus() {
        return status;
    }

    public void setStatus(V1FlinkJobStatus status) {
        this.status = status;
    }

    public V1FlinkJob status(V1FlinkJobStatus status) {
        this.status = status;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkJob that = (V1FlinkJob) o;
        return Objects.equals(getApiVersion(), that.getApiVersion()) &&
                Objects.equals(getKind(), that.getKind()) &&
                Objects.equals(getMetadata(), that.getMetadata()) &&
                Objects.equals(getStatus(), that.getStatus()) &&
                Objects.equals(getSpec(), that.getSpec());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getApiVersion(), getKind(), getMetadata(), getStatus(), getSpec());
    }

    @Override
    public String toString() {
        return "V1FlinkJob{" +
                "apiVersion='" + apiVersion + '\'' +
                ", kind='" + kind + '\'' +
                ", metadata=" + metadata +
                ", status=" + status +
                ", spec=" + spec +
                '}';
    }
}
