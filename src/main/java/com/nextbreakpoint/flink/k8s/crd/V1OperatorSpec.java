package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;

import java.util.Objects;

public class V1OperatorSpec {
    @SerializedName("savepointMode")
    private String savepointMode;
    @SerializedName("savepointPath")
    private String savepointPath;
    @SerializedName("savepointInterval")
    private Long savepointInterval;
    @SerializedName("savepointTargetPath")
    private String savepointTargetPath;
    @SerializedName("restartPolicy")
    private String restartPolicy;
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
    private String taskTimeout;
    @SerializedName("resources")
    private V1ResourceRequirements resources;

    public String getSavepointMode() {
        return savepointMode;
    }

    public V1OperatorSpec setSavepointMode(String savepointMode) {
        this.savepointMode = savepointMode;
        return this;
    }

    public String getSavepointPath() {
        return savepointPath;
    }

    public V1OperatorSpec setSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
        return this;
    }

    public Long getSavepointInterval() {
        return savepointInterval;
    }

    public V1OperatorSpec setSavepointInterval(Long savepointInterval) {
        this.savepointInterval = savepointInterval;
        return this;
    }

    public String getSavepointTargetPath() {
        return savepointTargetPath;
    }

    public void setSavepointTargetPath(String savepointTargetPath) {
        this.savepointTargetPath = savepointTargetPath;
    }

    public String getRestartPolicy() {
        return restartPolicy;
    }

    public void setRestartPolicy(String restartPolicy) {
        this.restartPolicy = restartPolicy;
    }

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

    public String getTaskTimeout() {
        return taskTimeout;
    }

    public void setTaskTimeout(String taskTimeout) {
        this.taskTimeout = taskTimeout;
    }

    public V1ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(V1ResourceRequirements resources) {
        this.resources = resources;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1OperatorSpec that = (V1OperatorSpec) o;
        return Objects.equals(getSavepointMode(), that.getSavepointMode()) &&
                Objects.equals(getSavepointPath(), that.getSavepointPath()) &&
                Objects.equals(getSavepointInterval(), that.getSavepointInterval()) &&
                Objects.equals(getSavepointTargetPath(), that.getSavepointTargetPath()) &&
                Objects.equals(getRestartPolicy(), that.getRestartPolicy()) &&
                Objects.equals(getPullSecrets(), that.getPullSecrets()) &&
                Objects.equals(getPullPolicy(), that.getPullPolicy()) &&
                Objects.equals(getImage(), that.getImage()) &&
                Objects.equals(getServiceAccount(), that.getServiceAccount()) &&
                Objects.equals(getPollingInterval(), that.getPollingInterval()) &&
                Objects.equals(getTaskTimeout(), that.getTaskTimeout()) &&
                Objects.equals(getResources(), that.getResources());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSavepointMode(), getSavepointPath(), getSavepointInterval(), getSavepointTargetPath(), getRestartPolicy(), getPullSecrets(), getPullPolicy(), getImage(), getServiceAccount(), getPollingInterval(), getTaskTimeout(), getResources());
    }

    @Override
    public String toString() {
        return "V1OperatorSpec{" +
                "savepointMode='" + savepointMode + '\'' +
                ", savepointPath='" + savepointPath + '\'' +
                ", savepointInterval=" + savepointInterval +
                ", savepointTargetPath='" + savepointTargetPath + '\'' +
                ", restartPolicy='" + restartPolicy + '\'' +
                ", pullSecrets='" + pullSecrets + '\'' +
                ", pullPolicy='" + pullPolicy + '\'' +
                ", image='" + image + '\'' +
                ", serviceAccount='" + serviceAccount + '\'' +
                ", pollingInterval='" + pollingInterval + '\'' +
                ", taskTimeout='" + taskTimeout + '\'' +
                ", resources=" + resources +
                '}';
    }
}
