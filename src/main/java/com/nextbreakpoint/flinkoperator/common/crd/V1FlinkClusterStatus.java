package com.nextbreakpoint.flinkoperator.common.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Arrays;
import java.util.Objects;

public class V1FlinkClusterStatus {
    @SerializedName("labelSelector")
    private String labelSelector;
    @SerializedName("taskManagers")
    private Integer taskManagers;
    @SerializedName("taskSlots")
    private Integer taskSlots;
    @SerializedName("jobParallelism")
    private Integer jobParallelism;
    @SerializedName("activeTaskManagers")
    private Integer activeTaskManagers;
    @SerializedName("totalTaskSlots")
    private Integer totalTaskSlots;
    @SerializedName("timestamp")
    private Long timestamp;
    @SerializedName("tasks")
    private String[] tasks;
    @SerializedName("taskStatus")
    private String taskStatus;
    @SerializedName("taskAttempts")
    private Integer taskAttempts;
    @SerializedName("clusterStatus")
    private String clusterStatus;
    @SerializedName("savepointPath")
    private String savepointPath;
    @SerializedName("savepointJobId")
    private String savepointJobId;
    @SerializedName("savepointTriggerId")
    private String savepointTriggerId;
    @SerializedName("savepointTimestamp")
    private Long savepointTimestamp;
    @SerializedName("digest")
    private V1ResourceDigest digest;

    public String getLabelSelector() {
        return labelSelector;
    }

    public void setLabelSelector(String labelSelector) {
        this.labelSelector = labelSelector;
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

    public Integer getJobParallelism() {
        return jobParallelism;
    }

    public void setJobParallelism(Integer jobParallelism) {
        this.jobParallelism = jobParallelism;
    }

    public Integer getActiveTaskManagers() {
        return activeTaskManagers;
    }

    public void setActiveTaskManagers(Integer activeTaskManagers) {
        this.activeTaskManagers = activeTaskManagers;
    }

    public Integer getTotalTaskSlots() {
        return totalTaskSlots;
    }

    public void setTotalTaskSlots(Integer totalTaskSlots) {
        this.totalTaskSlots = totalTaskSlots;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String[] getTasks() {
        return tasks;
    }

    public void setTasks(String[] tasks) {
        this.tasks = tasks;
    }

    public String getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
    }

    public Integer getTaskAttempts() {
        return taskAttempts;
    }

    public void setTaskAttempts(Integer taskAttempts) {
        this.taskAttempts = taskAttempts;
    }

    public String getClusterStatus() {
        return clusterStatus;
    }

    public void setClusterStatus(String clusterStatus) {
        this.clusterStatus = clusterStatus;
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

    public Long getSavepointTimestamp() {
        return savepointTimestamp;
    }

    public void setSavepointTimestamp(Long savepointTimestamp) {
        this.savepointTimestamp = savepointTimestamp;
    }

    public V1ResourceDigest getDigest() {
        return digest;
    }

    public void setDigest(V1ResourceDigest digest) {
        this.digest = digest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkClusterStatus that = (V1FlinkClusterStatus) o;
        return Objects.equals(getLabelSelector(), that.getLabelSelector()) &&
                Objects.equals(getTaskManagers(), that.getTaskManagers()) &&
                Objects.equals(getTaskSlots(), that.getTaskSlots()) &&
                Objects.equals(getJobParallelism(), that.getJobParallelism()) &&
                Objects.equals(getActiveTaskManagers(), that.getActiveTaskManagers()) &&
                Objects.equals(getTotalTaskSlots(), that.getTotalTaskSlots()) &&
                Objects.equals(getTimestamp(), that.getTimestamp()) &&
                Arrays.equals(getTasks(), that.getTasks()) &&
                Objects.equals(getTaskStatus(), that.getTaskStatus()) &&
                Objects.equals(getTaskAttempts(), that.getTaskAttempts()) &&
                Objects.equals(getClusterStatus(), that.getClusterStatus()) &&
                Objects.equals(getSavepointPath(), that.getSavepointPath()) &&
                Objects.equals(getSavepointJobId(), that.getSavepointJobId()) &&
                Objects.equals(getSavepointTriggerId(), that.getSavepointTriggerId()) &&
                Objects.equals(getSavepointTimestamp(), that.getSavepointTimestamp()) &&
                Objects.equals(getDigest(), that.getDigest());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(getLabelSelector(), getTaskManagers(), getTaskSlots(), getJobParallelism(), getActiveTaskManagers(), getTotalTaskSlots(), getTimestamp(), getTaskStatus(), getTaskAttempts(), getClusterStatus(), getSavepointPath(), getSavepointJobId(), getSavepointTriggerId(), getSavepointTimestamp(), getDigest());
        result = 31 * result + Arrays.hashCode(getTasks());
        return result;
    }

    @Override
    public String toString() {
        return "V1FlinkClusterStatus{" +
                "labelSelector='" + labelSelector + '\'' +
                ", taskManagers=" + taskManagers +
                ", taskSlots=" + taskSlots +
                ", jobParallelism=" + jobParallelism +
                ", activeTaskManagers=" + activeTaskManagers +
                ", totalTaskSlots=" + totalTaskSlots +
                ", timestamp=" + timestamp +
                ", tasks=" + Arrays.toString(tasks) +
                ", taskStatus='" + taskStatus + '\'' +
                ", taskAttempts=" + taskAttempts +
                ", clusterStatus='" + clusterStatus + '\'' +
                ", savepointPath='" + savepointPath + '\'' +
                ", savepointJobId='" + savepointJobId + '\'' +
                ", savepointTriggerId='" + savepointTriggerId + '\'' +
                ", savepointTimestamp=" + savepointTimestamp +
                ", digest=" + digest +
                '}';
    }
}
