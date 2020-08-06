package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V1FlinkJobSpec {
    @SerializedName("jobParallelism")
    private Integer jobParallelism;
    @SerializedName("bootstrap")
    private V1BootstrapSpec bootstrap;
    @SerializedName("savepoint")
    private V1SavepointSpec savepoint;

    public Integer getJobParallelism() {
        return jobParallelism;
    }

    public void setJobParallelism(Integer jobParallelism) {
        this.jobParallelism = jobParallelism;
    }

    public V1BootstrapSpec getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(V1BootstrapSpec bootstrap) {
        this.bootstrap = bootstrap;
    }

    public V1SavepointSpec getSavepoint() {
        return savepoint;
    }

    public void setSavepoint(V1SavepointSpec savepoint) {
        this.savepoint = savepoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkJobSpec that = (V1FlinkJobSpec) o;
        return Objects.equals(getJobParallelism(), that.getJobParallelism()) &&
                Objects.equals(getBootstrap(), that.getBootstrap()) &&
                Objects.equals(getSavepoint(), that.getSavepoint());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getJobParallelism(), getBootstrap(), getSavepoint());
    }

    @Override
    public String toString() {
        return "V1FlinkJobSpec{" +
                "jobParallelism=" + jobParallelism +
                ", bootstrap=" + bootstrap +
                ", savepoint=" + savepoint +
                '}';
    }
}
