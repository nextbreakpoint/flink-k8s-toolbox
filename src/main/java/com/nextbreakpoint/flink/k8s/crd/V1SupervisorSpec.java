package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;

import java.util.Objects;

public class V1SupervisorSpec {
    @SerializedName("pullSecrets")
    private String pullSecrets;
    @SerializedName("pullPolicy")
    private String pullPolicy;
    @SerializedName("image")
    private String image;
    @SerializedName("serviceAccount")
    private String serviceAccount;
    @SerializedName("pollingInterval")
    private String pollingInterval;
    @SerializedName("taskTimeout")
    private Integer taskTimeout;
    @SerializedName("maxTaskManagers")
    private Integer maxTaskManagers;
    @SerializedName("maxTaskSlots")
    private Integer maxTaskSlots;
    @SerializedName("resources")
    private V1ResourceRequirements resources;

    public String getPullSecrets() {
        return pullSecrets;
    }

    public void setPullSecrets(String pullSecrets) {
        this.pullSecrets = pullSecrets;
    }

    public String getPullPolicy() {
        return pullPolicy;
    }

    public void setPullPolicy(String pullPolicy) {
        this.pullPolicy = pullPolicy;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getServiceAccount() {
        return serviceAccount;
    }

    public void setServiceAccount(String serviceAccount) {
        this.serviceAccount = serviceAccount;
    }

    public String getPollingInterval() {
        return pollingInterval;
    }

    public void setPollingInterval(String pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    public Integer getTaskTimeout() {
        return taskTimeout;
    }

    public void setTaskTimeout(Integer taskTimeout) {
        this.taskTimeout = taskTimeout;
    }

    public V1ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(V1ResourceRequirements resources) {
        this.resources = resources;
    }

    public Integer getMaxTaskManagers() {
        return maxTaskManagers;
    }

    public void setMaxTaskManagers(Integer maxTaskManagers) {
        this.maxTaskManagers = maxTaskManagers;
    }

    public Integer getMaxTaskSlots() {
        return maxTaskSlots;
    }

    public void setMaxTaskSlots(Integer maxTaskSlots) {
        this.maxTaskSlots = maxTaskSlots;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1SupervisorSpec that = (V1SupervisorSpec) o;
        return Objects.equals(getPullSecrets(), that.getPullSecrets()) &&
                Objects.equals(getPullPolicy(), that.getPullPolicy()) &&
                Objects.equals(getImage(), that.getImage()) &&
                Objects.equals(getServiceAccount(), that.getServiceAccount()) &&
                Objects.equals(getPollingInterval(), that.getPollingInterval()) &&
                Objects.equals(getTaskTimeout(), that.getTaskTimeout()) &&
                Objects.equals(getResources(), that.getResources()) &&
                Objects.equals(getMaxTaskManagers(), that.getMaxTaskManagers()) &&
                Objects.equals(getMaxTaskSlots(), that.getMaxTaskSlots());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPullSecrets(), getPullPolicy(), getImage(), getServiceAccount(), getPollingInterval(), getTaskTimeout(), getResources(), getMaxTaskManagers(), getMaxTaskSlots());
    }

    @Override
    public String toString() {
        return "V1OperatorSpec{" +
                "pullSecrets='" + pullSecrets + '\'' +
                ", pullPolicy='" + pullPolicy + '\'' +
                ", image='" + image + '\'' +
                ", serviceAccount='" + serviceAccount + '\'' +
                ", pollingInterval='" + pollingInterval + '\'' +
                ", taskTimeout='" + taskTimeout + '\'' +
                ", resources=" + resources +
                ", maxTaskManagers=" + maxTaskManagers +
                ", maxTaskSlots=" + maxTaskSlots +
                '}';
    }
}
