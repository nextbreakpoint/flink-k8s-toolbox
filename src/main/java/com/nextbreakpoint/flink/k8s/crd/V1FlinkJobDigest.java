package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V1FlinkJobDigest {
    @SerializedName("bootstrap")
    private String bootstrap;

    public String getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkJobDigest that = (V1FlinkJobDigest) o;
        return Objects.equals(getBootstrap(), that.getBootstrap());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBootstrap());
    }

    @Override
    public String toString() {
        return "V1FlinkJobDigest{" +
                "bootstrap='" + bootstrap + '\'' +
                '}';
    }
}
