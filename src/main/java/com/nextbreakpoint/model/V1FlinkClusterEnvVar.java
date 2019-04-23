package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V1FlinkClusterEnvVar {
    @SerializedName("name")
    private String name = null;
    @SerializedName("value")
    private String value = null;

    public String getName() {
        return name;
    }

    public V1FlinkClusterEnvVar setName(String name) {
        this.name = name;
        return this;
    }

    public String getValue() {
        return value;
    }

    public V1FlinkClusterEnvVar setValue(String value) {
        this.value = value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkClusterEnvVar that = (V1FlinkClusterEnvVar) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return "V1FlinkClusterEnvVar {" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
