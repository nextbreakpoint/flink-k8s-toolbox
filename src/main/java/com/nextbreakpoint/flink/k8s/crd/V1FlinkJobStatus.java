package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import org.joda.time.DateTime;

import java.util.Objects;

public class V1FlinkJobStatus {
    @SerializedName("resourceStatus")
    private String resourceStatus;
    @SerializedName("supervisorStatus")
    private String supervisorStatus;
    @SerializedName("clusterName")
    private String clusterName;
    @SerializedName("clusterUid")
    private String clusterUid;
    @SerializedName("clusterHealth")
    private String clusterHealth;
    @SerializedName("labelSelector")
    private String labelSelector;
    @SerializedName("jobParallelism")
    private Integer jobParallelism;
    @SerializedName("timestamp")
    private DateTime timestamp;
    @SerializedName("jobId")
    private String jobId;
    @SerializedName("jobStatus")
    private String jobStatus;
    @SerializedName("savepointPath")
    private String savepointPath;
    @SerializedName("savepointJobId")
    private String savepointJobId;
    @SerializedName("savepointTriggerId")
    private String savepointTriggerId;
    @SerializedName("savepointTimestamp")
    private DateTime savepointTimestamp;
    @SerializedName("savepointRequestTimestamp")
    private DateTime savepointRequestTimestamp;
    @SerializedName("savepointMode")
    private String savepointMode;
    @SerializedName("restartPolicy")
    private String restartPolicy;
    @SerializedName("digest")
    private V1FlinkJobDigest digest;
    @SerializedName("bootstrap")
    private V1BootstrapSpec bootstrap;

    public String getResourceStatus() {
        return resourceStatus;
    }

    public void setResourceStatus(String resourceStatus) {
        this.resourceStatus = resourceStatus;
    }

    public String getSupervisorStatus() {
        return supervisorStatus;
    }

    public void setSupervisorStatus(String supervisorStatus) {
        this.supervisorStatus = supervisorStatus;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterUid() {
        return clusterUid;
    }

    public void setClusterUid(String clusterUid) {
        this.clusterUid = clusterUid;
    }

    public String getClusterHealth() {
        return clusterHealth;
    }

    public void setClusterHealth(String clusterHealth) {
        this.clusterHealth = clusterHealth;
    }

    public String getLabelSelector() {
        return labelSelector;
    }

    public void setLabelSelector(String labelSelector) {
        this.labelSelector = labelSelector;
    }

    public Integer getJobParallelism() {
        return jobParallelism;
    }

    public void setJobParallelism(Integer jobParallelism) {
        this.jobParallelism = jobParallelism;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(DateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String jobStatus) {
        this.jobStatus = jobStatus;
    }

    public String getSavepointPath() {
        return savepointPath;
    }

    public void setSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
    }

    public String getSavepointJobId() {
        return savepointJobId;
    }

    public void setSavepointJobId(String savepointJobId) {
        this.savepointJobId = savepointJobId;
    }

    public String getSavepointTriggerId() {
        return savepointTriggerId;
    }

    public void setSavepointTriggerId(String savepointTriggerId) {
        this.savepointTriggerId = savepointTriggerId;
    }

    public DateTime getSavepointTimestamp() {
        return savepointTimestamp;
    }

    public void setSavepointTimestamp(DateTime savepointTimestamp) {
        this.savepointTimestamp = savepointTimestamp;
    }

    public DateTime getSavepointRequestTimestamp() {
        return savepointRequestTimestamp;
    }

    public void setSavepointRequestTimestamp(DateTime savepointRequestTimestamp) {
        this.savepointRequestTimestamp = savepointRequestTimestamp;
    }

    public String getSavepointMode() {
        return savepointMode;
    }

    public void setSavepointMode(String savepointMode) {
        this.savepointMode = savepointMode;
    }

    public String getRestartPolicy() {
        return restartPolicy;
    }

    public void setRestartPolicy(String restartPolicy) {
        this.restartPolicy = restartPolicy;
    }

    public V1FlinkJobDigest getDigest() {
        return digest;
    }

    public void setDigest(V1FlinkJobDigest digest) {
        this.digest = digest;
    }

    public V1BootstrapSpec getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(V1BootstrapSpec bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkJobStatus that = (V1FlinkJobStatus) o;
        return Objects.equals(getResourceStatus(), that.getResourceStatus()) &&
                Objects.equals(getSupervisorStatus(), that.getSupervisorStatus()) &&
                Objects.equals(getClusterName(), that.getClusterName()) &&
                Objects.equals(getClusterUid(), that.getClusterUid()) &&
                Objects.equals(getClusterHealth(), that.getClusterHealth()) &&
                Objects.equals(getLabelSelector(), that.getLabelSelector()) &&
                Objects.equals(getJobParallelism(), that.getJobParallelism()) &&
                Objects.equals(getTimestamp(), that.getTimestamp()) &&
                Objects.equals(getJobId(), that.getJobId()) &&
                Objects.equals(getJobStatus(), that.getJobStatus()) &&
                Objects.equals(getSavepointPath(), that.getSavepointPath()) &&
                Objects.equals(getSavepointJobId(), that.getSavepointJobId()) &&
                Objects.equals(getSavepointTriggerId(), that.getSavepointTriggerId()) &&
                Objects.equals(getSavepointTimestamp(), that.getSavepointTimestamp()) &&
                Objects.equals(getSavepointRequestTimestamp(), that.getSavepointRequestTimestamp()) &&
                Objects.equals(getSavepointMode(), that.getSavepointMode()) &&
                Objects.equals(getRestartPolicy(), that.getRestartPolicy()) &&
                Objects.equals(getDigest(), that.getDigest()) &&
                Objects.equals(getBootstrap(), that.getBootstrap());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(getResourceStatus(), getSupervisorStatus(), getClusterName(), getClusterUid(), getClusterHealth(), getLabelSelector(), getJobParallelism(), getTimestamp(), getJobId(), getJobStatus(), getSavepointPath(), getSavepointJobId(), getSavepointTriggerId(), getSavepointTimestamp(), getSavepointRequestTimestamp(), getSavepointMode(), getRestartPolicy(), getDigest(), getBootstrap());
        return result;
    }

    @Override
    public String toString() {
        return "V1FlinkJobStatus{" +
                "resourceStatus='" + resourceStatus + '\'' +
                ", supervisorStatus='" + supervisorStatus + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", clusterUid='" + clusterUid + '\'' +
                ", clusterHealth='" + clusterHealth + '\'' +
                ", labelSelector='" + labelSelector + '\'' +
                ", jobParallelism=" + jobParallelism +
                ", timestamp=" + timestamp +
                ", jobId='" + jobId + '\'' +
                ", jobStatus='" + jobStatus + '\'' +
                ", savepointPath='" + savepointPath + '\'' +
                ", savepointJobId='" + savepointJobId + '\'' +
                ", savepointTriggerId='" + savepointTriggerId + '\'' +
                ", savepointTimestamp=" + savepointTimestamp +
                ", savepointRequestTimestamp=" + savepointRequestTimestamp +
                ", savepointMode='" + savepointMode + '\'' +
                ", restartPolicy='" + restartPolicy + '\'' +
                ", digest=" + digest +
                ", bootstrap=" + bootstrap +
                '}';
    }
}
