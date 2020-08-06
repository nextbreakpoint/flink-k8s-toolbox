package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import java.util.Objects;

public class V2FlinkCluster {
    @SerializedName("apiVersion")
    private String apiVersion = null;
    @SerializedName("kind")
    private String kind = null;
    @SerializedName("metadata")
    private V1ObjectMeta metadata = null;
    @SerializedName("status")
    private V2FlinkClusterStatus status = null;
    @SerializedName("spec")
    private V2FlinkClusterSpec spec = null;

    public V2FlinkCluster() {
    }

    public V2FlinkCluster apiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
        return this;
    }

    public String getApiVersion() {
        return this.apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public V2FlinkCluster kind(String kind) {
        this.kind = kind;
        return this;
    }

    public String getKind() {
        return this.kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public V2FlinkCluster metadata(V1ObjectMeta metadata) {
        this.metadata = metadata;
        return this;
    }

    public V1ObjectMeta getMetadata() {
        return this.metadata;
    }

    public void setMetadata(V1ObjectMeta metadata) {
        this.metadata = metadata;
    }

    public V2FlinkClusterSpec getSpec() {
        return spec;
    }

    public void setSpec(V2FlinkClusterSpec spec) {
        this.spec = spec;
    }

    public V2FlinkCluster spec(V2FlinkClusterSpec spec) {
        this.spec = spec;
        return this;
    }

    public V2FlinkClusterStatus getStatus() {
        return status;
    }

    public void setStatus(V2FlinkClusterStatus status) {
        this.status = status;
    }

    public V2FlinkCluster status(V2FlinkClusterStatus status) {
        this.status = status;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V2FlinkCluster that = (V2FlinkCluster) o;
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
        return "V2FlinkCluster{" +
                "apiVersion='" + apiVersion + '\'' +
                ", kind='" + kind + '\'' +
                ", metadata=" + metadata +
                ", status=" + status +
                ", spec=" + spec +
                '}';
    }
}
