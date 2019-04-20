package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.models.V1ListMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class V1FlinkClusterList {
    @SerializedName("apiVersion")
    private String apiVersion = null;
    @SerializedName("items")
    private List<V1FlinkCluster> items = new ArrayList<>();
    @SerializedName("kind")
    private String kind = null;
    @SerializedName("metadata")
    private V1ListMeta metadata = null;

    public V1FlinkClusterList() {
    }

    public V1FlinkClusterList apiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
        return this;
    }

    public String getApiVersion() {
        return this.apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public V1FlinkClusterList items(List<V1FlinkCluster> items) {
        this.items = items;
        return this;
    }

    public V1FlinkClusterList addItemsItem(V1FlinkCluster itemsItem) {
        this.items.add(itemsItem);
        return this;
    }

    public List<V1FlinkCluster> getItems() {
        return this.items;
    }

    public void setItems(List<V1FlinkCluster> items) {
        this.items = items;
    }

    public V1FlinkClusterList kind(String kind) {
        this.kind = kind;
        return this;
    }

    public String getKind() {
        return this.kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public V1FlinkClusterList metadata(V1ListMeta metadata) {
        this.metadata = metadata;
        return this;
    }

    public V1ListMeta getMetadata() {
        return this.metadata;
    }

    public void setMetadata(V1ListMeta metadata) {
        this.metadata = metadata;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            V1FlinkClusterList V1FlinkClusterList = (V1FlinkClusterList) o;
            return Objects.equals(this.apiVersion, V1FlinkClusterList.apiVersion) && Objects.equals(this.items, V1FlinkClusterList.items) && Objects.equals(this.kind, V1FlinkClusterList.kind) && Objects.equals(this.metadata, V1FlinkClusterList.metadata);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(this.apiVersion, this.items, this.kind, this.metadata);
    }

    public String toString() {
        return "class V1FlinkClusterList {\n" +
                "    apiVersion: " + this.toIndentedString(this.apiVersion) + "\n" +
                "    items: " + this.toIndentedString(this.items) + "\n" +
                "    kind: " + this.toIndentedString(this.kind) + "\n" +
                "    metadata: " + this.toIndentedString(this.metadata) + "\n" +
                "}";
    }

    private String toIndentedString(Object o) {
        return o == null ? "null" : o.toString().replace("\n", "\n    ");
    }
}