package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Objects;

public class V2FlinkClusterStatus {
    @SerializedName("resourceStatus")
    private String resourceStatus;
    @SerializedName("supervisorStatus")
    private String supervisorStatus;
    @SerializedName("labelSelector")
    private String labelSelector;
    @SerializedName("clusterHealth")
    private String clusterHealth;
    @SerializedName("taskManagers")
    private Integer taskManagers;
    @SerializedName("taskSlots")
    private Integer taskSlots;
    @SerializedName("taskManagerReplicas")
    private Integer taskManagerReplicas;
    @SerializedName("totalTaskSlots")
    private Integer totalTaskSlots;
    @SerializedName("timestamp")
    private DateTime timestamp;
    @SerializedName("serviceMode")
    private String serviceMode;
    @SerializedName("digest")
    private V2FlinkClusterDigest digest;
    @SerializedName("jobs")
    private List<V2FlinkClusterJobDigest> jobs;
    @SerializedName("rescaleTimestamp")
    private DateTime rescaleTimestamp;

    public String getResourceStatus() {
        return resourceStatus;
    }

    public void setResourceStatus(String resourceStatus) {
        this.resourceStatus = resourceStatus;
    }

    public String getLabelSelector() {
        return labelSelector;
    }

    public void setLabelSelector(String labelSelector) {
        this.labelSelector = labelSelector;
    }

    public String getClusterHealth() {
        return clusterHealth;
    }

    public void setClusterHealth(String clusterHealth) {
        this.clusterHealth = clusterHealth;
    }

    public Integer getTaskManagers() {
        return taskManagers;
    }

    public void setTaskManagers(Integer taskManagers) {
        this.taskManagers = taskManagers;
    }

    public Integer getTaskSlots() {
        return taskSlots;
    }

    public void setTaskSlots(Integer taskSlots) {
        this.taskSlots = taskSlots;
    }

    public Integer getTaskManagerReplicas() {
        return taskManagerReplicas;
    }

    public void setTaskManagerReplicas(Integer taskManagerReplicas) {
        this.taskManagerReplicas = taskManagerReplicas;
    }

    public Integer getTotalTaskSlots() {
        return totalTaskSlots;
    }

    public void setTotalTaskSlots(Integer totalTaskSlots) {
        this.totalTaskSlots = totalTaskSlots;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(DateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getSupervisorStatus() {
        return supervisorStatus;
    }

    public void setSupervisorStatus(String supervisorStatus) {
        this.supervisorStatus = supervisorStatus;
    }

    public String getServiceMode() {
        return serviceMode;
    }

    public void setServiceMode(String serviceMode) {
        this.serviceMode = serviceMode;
    }

    public V2FlinkClusterDigest getDigest() {
        return digest;
    }

    public void setDigest(V2FlinkClusterDigest digest) {
        this.digest = digest;
    }

    public List<V2FlinkClusterJobDigest> getJobs() {
        return jobs;
    }

    public void setJobs(List<V2FlinkClusterJobDigest> jobs) {
        this.jobs = jobs;
    }

    public DateTime getRescaleTimestamp() {
        return rescaleTimestamp;
    }

    public void setRescaleTimestamp(DateTime rescaleTimestamp) {
        this.rescaleTimestamp = rescaleTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V2FlinkClusterStatus that = (V2FlinkClusterStatus) o;
        return Objects.equals(getResourceStatus(), that.getResourceStatus()) &&
                Objects.equals(getLabelSelector(), that.getLabelSelector()) &&
                Objects.equals(getClusterHealth(), that.getClusterHealth()) &&
                Objects.equals(getTaskManagers(), that.getTaskManagers()) &&
                Objects.equals(getTaskSlots(), that.getTaskSlots()) &&
                Objects.equals(getTaskManagerReplicas(), that.getTaskManagerReplicas()) &&
                Objects.equals(getTotalTaskSlots(), that.getTotalTaskSlots()) &&
                Objects.equals(getTimestamp(), that.getTimestamp()) &&
                Objects.equals(getSupervisorStatus(), that.getSupervisorStatus()) &&
                Objects.equals(getServiceMode(), that.getServiceMode()) &&
                Objects.equals(getDigest(), that.getDigest()) &&
                Objects.equals(getJobs(), that.getJobs()) &&
                Objects.equals(getRescaleTimestamp(), that.getRescaleTimestamp());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(getResourceStatus(), getSupervisorStatus(), getLabelSelector(), getClusterHealth(), getTaskManagers(), getTaskSlots(), getTaskManagerReplicas(), getTotalTaskSlots(), getTimestamp(), getServiceMode(), getDigest(), getJobs(), getRescaleTimestamp());
        return result;
    }

    @Override
    public String toString() {
        return "V2FlinkClusterStatus{" +
                "resourceStatus='" + resourceStatus + '\'' +
                ", supervisorStatus='" + supervisorStatus + '\'' +
                ", labelSelector='" + labelSelector + '\'' +
                ", clusterHealth='" + clusterHealth + '\'' +
                ", taskManagers=" + taskManagers +
                ", taskSlots=" + taskSlots +
                ", taskManagerReplicas=" + taskManagerReplicas +
                ", totalTaskSlots=" + totalTaskSlots +
                ", timestamp=" + timestamp +
                ", serviceMode='" + serviceMode + '\'' +
                ", digest=" + digest +
                ", jobs=" + jobs +
                ", rescaleTimestamp=" + rescaleTimestamp +
                '}';
    }
}
