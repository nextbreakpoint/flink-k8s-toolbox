package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ListMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class V1FlinkJobList {
    @SerializedName("apiVersion")
    private String apiVersion = null;
    @SerializedName("items")
    private List<V1FlinkJob> items = new ArrayList<>();
    @SerializedName("kind")
    private String kind = null;
    @SerializedName("metadata")
    private V1ListMeta metadata = null;

    public V1FlinkJobList() {
    }

    public V1FlinkJobList apiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
        return this;
    }

    public String getApiVersion() {
        return this.apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public V1FlinkJobList items(List<V1FlinkJob> items) {
        this.items = items;
        return this;
    }

    public V1FlinkJobList addItemsItem(V1FlinkJob itemsItem) {
        this.items.add(itemsItem);
        return this;
    }

    public List<V1FlinkJob> getItems() {
        return this.items;
    }

    public void setItems(List<V1FlinkJob> items) {
        this.items = items;
    }

    public V1FlinkJobList kind(String kind) {
        this.kind = kind;
        return this;
    }

    public String getKind() {
        return this.kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public V1FlinkJobList metadata(V1ListMeta metadata) {
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
        V1FlinkJobList that = (V1FlinkJobList) o;
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
        return "V2FlinkJobList{" +
                "apiVersion='" + apiVersion + '\'' +
                ", items=" + items +
                ", kind='" + kind + '\'' +
                ", metadata=" + metadata +
                '}';
    }
}