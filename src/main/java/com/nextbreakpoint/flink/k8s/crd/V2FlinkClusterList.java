package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ListMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class V2FlinkClusterList {
    @SerializedName("apiVersion")
    private String apiVersion = null;
    @SerializedName("items")
    private List<V2FlinkCluster> items = new ArrayList<>();
    @SerializedName("kind")
    private String kind = null;
    @SerializedName("metadata")
    private V1ListMeta metadata = null;

    public V2FlinkClusterList() {
    }

    public V2FlinkClusterList apiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
        return this;
    }

    public String getApiVersion() {
        return this.apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public V2FlinkClusterList items(List<V2FlinkCluster> items) {
        this.items = items;
        return this;
    }

    public V2FlinkClusterList addItemsItem(V2FlinkCluster itemsItem) {
        this.items.add(itemsItem);
        return this;
    }

    public List<V2FlinkCluster> getItems() {
        return this.items;
    }

    public void setItems(List<V2FlinkCluster> items) {
        this.items = items;
    }

    public V2FlinkClusterList kind(String kind) {
        this.kind = kind;
        return this;
    }

    public String getKind() {
        return this.kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public V2FlinkClusterList metadata(V1ListMeta metadata) {
        this.metadata = metadata;
        return this;
    }

    public V1ListMeta getMetadata() {
        return this.metadata;
    }

    public void setMetadata(V1ListMeta metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V2FlinkClusterList that = (V2FlinkClusterList) o;
        return Objects.equals(getApiVersion(), that.getApiVersion()) &&
                Objects.equals(getItems(), that.getItems()) &&
                Objects.equals(getKind(), that.getKind()) &&
                Objects.equals(getMetadata(), that.getMetadata());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getApiVersion(), getItems(), getKind(), getMetadata());
    }

    @Override
    public String toString() {
        return "V2FlinkClusterList{" +
                "apiVersion='" + apiVersion + '\'' +
                ", items=" + items +
                ", kind='" + kind + '\'' +
                ", metadata=" + metadata +
                '}';
    }
}