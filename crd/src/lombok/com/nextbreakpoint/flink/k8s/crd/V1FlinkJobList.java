package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.openapi.models.V1ListMeta;
import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkJobList implements KubernetesListObject {
    @SerializedName("apiVersion")
    private String apiVersion;
    @SerializedName("items")
    @Builder.Default
    private List<V1FlinkJob> items = new ArrayList<>();
    @SerializedName("kind")
    private String kind;
    @SerializedName("metadata")
    private V1ListMeta metadata;

    public V1FlinkJobList items(List<V1FlinkJob> items) {
        this.items = items;
        return this;
    }

    public V1FlinkJobList addItemsItem(V1FlinkJob itemsItem) {
        this.items.add(itemsItem);
        return this;
    }
}