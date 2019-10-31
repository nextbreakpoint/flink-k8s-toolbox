package com.nextbreakpoint.flinkoperator.common.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Arrays;
import java.util.Objects;

public class V1FlinkClusterState {
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
    @SerializedName("digestOfJobManager")
    private String digestOfJobManager;
    @SerializedName("digestOfTaskManager")
    private String digestOfTaskManager;
    @SerializedName("digestOfFlinkImage")
    private String digestOfFlinkImage;
    @SerializedName("digestOfFlinkJob")
    private String digestOfFlinkJob;

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

    public String getDigestOfJobManager() {
        return digestOfJobManager;
    }

    public void setDigestOfJobManager(String digestOfJobManager) {
        this.digestOfJobManager = digestOfJobManager;
    }

    public String getDigestOfTaskManager() {
        return digestOfTaskManager;
    }

    public void setDigestOfTaskManager(String digestOfTaskManager) {
        this.digestOfTaskManager = digestOfTaskManager;
    }

    public String getDigestOfFlinkImage() {
        return digestOfFlinkImage;
    }

    public void setDigestOfFlinkImage(String digestOfFlinkImage) {
        this.digestOfFlinkImage = digestOfFlinkImage;
    }

    public String getDigestOfFlinkJob() {
        return digestOfFlinkJob;
    }

    public void setDigestOfFlinkJob(String digestOfFlinkJob) {
        this.digestOfFlinkJob = digestOfFlinkJob;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkClusterState that = (V1FlinkClusterState) o;
        return Objects.equals(getTimestamp(), that.getTimestamp()) &&
                Arrays.equals(getTasks(), that.getTasks()) &&
                Objects.equals(getTaskStatus(), that.getTaskStatus()) &&
                Objects.equals(getTaskAttempts(), that.getTaskAttempts()) &&
                Objects.equals(getClusterStatus(), that.getClusterStatus()) &&
                Objects.equals(getSavepointPath(), that.getSavepointPath()) &&
                Objects.equals(getSavepointJobId(), that.getSavepointJobId()) &&
                Objects.equals(getSavepointTriggerId(), that.getSavepointTriggerId()) &&
                Objects.equals(getSavepointTimestamp(), that.getSavepointTimestamp()) &&
                Objects.equals(getDigestOfJobManager(), that.getDigestOfJobManager()) &&
                Objects.equals(getDigestOfTaskManager(), that.getDigestOfTaskManager()) &&
                Objects.equals(getDigestOfFlinkImage(), that.getDigestOfFlinkImage()) &&
                Objects.equals(getDigestOfFlinkJob(), that.getDigestOfFlinkJob());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(getTimestamp(), getTaskStatus(), getTaskAttempts(), getClusterStatus(), getSavepointPath(), getSavepointJobId(), getSavepointTriggerId(), getSavepointTimestamp(), getDigestOfJobManager(), getDigestOfTaskManager(), getDigestOfFlinkImage(), getDigestOfFlinkJob());
        result = 31 * result + Arrays.hashCode(getTasks());
        return result;
    }

    @Override
    public String toString() {
        return "V1FlinkClusterState{" +
                "timestamp='" + timestamp + '\'' +
                ", tasks=" + Arrays.toString(tasks) +
                ", taskStatus='" + taskStatus + '\'' +
                ", taskAttempts=" + taskAttempts +
                ", clusterStatus='" + clusterStatus + '\'' +
                ", savepointPath='" + savepointPath + '\'' +
                ", savepointJobId='" + savepointJobId + '\'' +
                ", savepointTriggerId='" + savepointTriggerId + '\'' +
                ", savepointTimestamp='" + savepointTimestamp + '\'' +
                ", digestOfJobManager='" + digestOfJobManager + '\'' +
                ", digestOfTaskManager='" + digestOfTaskManager + '\'' +
                ", digestOfFlinkImage='" + digestOfFlinkImage + '\'' +
                ", digestOfFlinkJob='" + digestOfFlinkJob + '\'' +
                '}';
    }
}
