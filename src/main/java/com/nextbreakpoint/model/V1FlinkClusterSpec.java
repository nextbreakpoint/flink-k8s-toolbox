package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V1FlinkClusterSpec {
    @SerializedName("pullSecrets")
    private String pullSecrets;
    @SerializedName("pullPolicy")
    private String pullPolicy;
    @SerializedName("flinkImage")
    private String flinkImage;
    @SerializedName("serviceAccount")
    private String serviceAccount;
    @SerializedName("jobManagerSpec")
    private V1JobManagerSpec jobManagerSpec;
    @SerializedName("taskManagerSpec")
    private V1TaskManagerSpec taskManagerSpec;
    @SerializedName("flinkJobSpec")
    private V1FlinkJobSpec flinkJobSpec;

    public String getPullPolicy() {
        return pullPolicy;
    }

    public V1FlinkClusterSpec setPullPolicy(String pullPolicy) {
        this.pullPolicy = pullPolicy;
        return this;
    }

    public String getPullSecrets() {
        return pullSecrets;
    }

    public V1FlinkClusterSpec setPullSecrets(String pullSecrets) {
        this.pullSecrets = pullSecrets;
        return this;
    }

    public String getFlinkImage() {
        return flinkImage;
    }

    public V1FlinkClusterSpec setFlinkImage(String flinkImage) {
        this.flinkImage = flinkImage;
        return this;
    }

    public String getServiceAccount() {
        return serviceAccount;
    }

    public V1FlinkClusterSpec setServiceAccount(String serviceAccount) {
        this.serviceAccount = serviceAccount;
        return this;
    }

    public V1JobManagerSpec getJobManagerSpec() {
        return jobManagerSpec;
    }

    public V1FlinkClusterSpec setJobManagerSpec(V1JobManagerSpec jobManagerSpec) {
        this.jobManagerSpec = jobManagerSpec;
        return this;
    }

    public V1TaskManagerSpec getTaskManagerSpec() {
        return taskManagerSpec;
    }

    public V1FlinkClusterSpec setTaskManagerSpec(V1TaskManagerSpec taskManagerSpec) {
        this.taskManagerSpec = taskManagerSpec;
        return this;
    }

    public V1FlinkJobSpec getFlinkJobSpec() {
        return flinkJobSpec;
    }

    public V1FlinkClusterSpec setFlinkJobSpec(V1FlinkJobSpec flinkJobSpec) {
        this.flinkJobSpec = flinkJobSpec;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkClusterSpec that = (V1FlinkClusterSpec) o;
        return Objects.equals(getPullSecrets(), that.getPullSecrets()) &&
                Objects.equals(getPullPolicy(), that.getPullPolicy()) &&
                Objects.equals(getFlinkImage(), that.getFlinkImage()) &&
                Objects.equals(getServiceAccount(), that.getServiceAccount()) &&
                Objects.equals(getJobManagerSpec(), that.getJobManagerSpec()) &&
                Objects.equals(getTaskManagerSpec(), that.getTaskManagerSpec()) &&
                Objects.equals(getFlinkJobSpec(), that.getFlinkJobSpec());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPullSecrets(), getPullPolicy(), getFlinkImage(), getServiceAccount(), getJobManagerSpec(), getTaskManagerSpec(), getFlinkJobSpec());
    }

    @Override
    public String toString() {
        return "V1FlinkClusterSpec{" +
                "pullSecrets='" + pullSecrets + '\'' +
                ", pullPolicy='" + pullPolicy + '\'' +
                ", flinkImage='" + flinkImage + '\'' +
                ", serviceAccount='" + serviceAccount + '\'' +
                ", jobManagerSpec=" + jobManagerSpec +
                ", taskManagerSpec=" + taskManagerSpec +
                ", flinkJobSpec=" + flinkJobSpec +
                '}';
    }
}
