package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V2FlinkClusterJobSpec {
    @SerializedName("name")
    private String name;
    @SerializedName("spec")
    private V1FlinkJobSpec spec;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public V1FlinkJobSpec getSpec() {
        return spec;
    }

    public void setSpec(V1FlinkJobSpec spec) {
        this.spec = spec;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V2FlinkClusterJobSpec that = (V2FlinkClusterJobSpec) o;
        return Objects.equals(getName(), that.getName()) &&
                Objects.equals(getSpec(), that.getSpec());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getSpec());
    }

    @Override
    public String toString() {
        return "V2FlinkClusterJobSpec{" +
                "name='" + name + '\'' +
                ", spec=" + spec +
                '}';
    }
}