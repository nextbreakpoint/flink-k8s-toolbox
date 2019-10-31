package com.nextbreakpoint.flinkoperator.common.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.models.V1ObjectMeta;

import java.util.Objects;

public class V1FlinkCluster {
    @SerializedName("apiVersion")
    private String apiVersion = null;
    @SerializedName("kind")
    private String kind = null;
    @SerializedName("metadata")
    private V1ObjectMeta metadata = null;
    @SerializedName("state")
    private V1FlinkClusterState state = null;
    @SerializedName("spec")
    private V1FlinkClusterSpec spec = null;

    public V1FlinkCluster() {
    }

    public V1FlinkCluster apiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
        return this;
    }

    public String getApiVersion() {
        return this.apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public V1FlinkCluster kind(String kind) {
        this.kind = kind;
        return this;
    }

    public String getKind() {
        return this.kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public V1FlinkCluster metadata(V1ObjectMeta metadata) {
        this.metadata = metadata;
        return this;
    }

    public V1ObjectMeta getMetadata() {
        return this.metadata;
    }

    public void setMetadata(V1ObjectMeta metadata) {
        this.metadata = metadata;
    }

    public V1FlinkClusterSpec getSpec() {
        return spec;
    }

    public void setSpec(V1FlinkClusterSpec spec) {
        this.spec = spec;
    }

    public V1FlinkCluster spec(V1FlinkClusterSpec spec) {
        this.spec = spec;
        return this;
    }

    public V1FlinkClusterState getState() {
        return state;
    }

    public void setState(V1FlinkClusterState state) {
        this.state = state;
    }

    public V1FlinkCluster state(V1FlinkClusterState state) {
        this.state = state;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkCluster that = (V1FlinkCluster) o;
        return Objects.equals(getApiVersion(), that.getApiVersion()) &&
                Objects.equals(getKind(), that.getKind()) &&
                Objects.equals(getMetadata(), that.getMetadata()) &&
                Objects.equals(getState(), that.getState()) &&
                Objects.equals(getSpec(), that.getSpec());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getApiVersion(), getKind(), getMetadata(), getState(), getSpec());
    }

    @Override
    public String toString() {
        return "V1FlinkCluster{" +
                "apiVersion='" + apiVersion + '\'' +
                ", kind='" + kind + '\'' +
                ", metadata=" + metadata +
                ", state=" + state +
                ", spec=" + spec +
                '}';
    }
}
