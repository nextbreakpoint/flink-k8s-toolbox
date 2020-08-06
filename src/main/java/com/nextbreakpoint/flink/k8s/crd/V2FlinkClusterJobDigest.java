package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V2FlinkClusterJobDigest {
    @SerializedName("name")
    private String name;
    @SerializedName("digest")
    private String digest;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDigest() {
        return digest;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V2FlinkClusterJobDigest that = (V2FlinkClusterJobDigest) o;
        return Objects.equals(getName(), that.getName()) &&
                Objects.equals(getDigest(), that.getDigest());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getDigest());
    }

    @Override
    public String toString() {
        return "V2FlinkClusterJobDigest{" +
                "name='" + name + '\'' +
                ", digest='" + digest + '\'' +
                '}';
    }
}
