package com.nextbreakpoint.flinkoperator.common.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V1FlinkClusterSpec {
    @SerializedName("jobManager")
    private V1JobManagerSpec jobManager;
    @SerializedName("taskManager")
    private V1TaskManagerSpec taskManager;
    @SerializedName("flinkImage")
    private V1FlinkImageSpec flinkImage;
    @SerializedName("flinkJob")
    private V1FlinkJobSpec flinkJob;
    @SerializedName("taskManagers")
    private Integer taskManagers;
    @SerializedName("operator")
    private V1OperatorSpec operator;

    public V1JobManagerSpec getJobManager() {
        return jobManager;
    }

    public V1FlinkClusterSpec setJobManager(V1JobManagerSpec jobManager) {
        this.jobManager = jobManager;
        return this;
    }

    public V1TaskManagerSpec getTaskManager() {
        return taskManager;
    }

    public V1FlinkClusterSpec setTaskManager(V1TaskManagerSpec taskManager) {
        this.taskManager = taskManager;
        return this;
    }

    public V1FlinkImageSpec getFlinkImage() {
        return flinkImage;
    }

    public V1FlinkClusterSpec setFlinkImage(V1FlinkImageSpec flinkImage) {
        this.flinkImage = flinkImage;
        return this;
    }

    public V1FlinkJobSpec getFlinkJob() {
        return flinkJob;
    }

    public V1FlinkClusterSpec setFlinkJob(V1FlinkJobSpec flinkJob) {
        this.flinkJob = flinkJob;
        return this;
    }

    public V1OperatorSpec getOperator() {
        return operator;
    }

    public void setOperator(V1OperatorSpec operator) {
        this.operator = operator;
    }

    public Integer getTaskManagers() {
        return taskManagers;
    }

    public void setTaskManagers(Integer taskManagers) {
        this.taskManagers = taskManagers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkClusterSpec that = (V1FlinkClusterSpec) o;
        return Objects.equals(getJobManager(), that.getJobManager()) &&
                Objects.equals(getTaskManager(), that.getTaskManager()) &&
                Objects.equals(getFlinkImage(), that.getFlinkImage()) &&
                Objects.equals(getFlinkJob(), that.getFlinkJob()) &&
                Objects.equals(getOperator(), that.getOperator()) &&
                Objects.equals(getTaskManagers(), that.getTaskManagers());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getJobManager(), getTaskManager(), getFlinkImage(), getFlinkJob(), getOperator(), getTaskManagers());
    }

    @Override
    public String toString() {
        return "V1FlinkClusterSpec{" +
                ", jobManager=" + jobManager +
                ", taskManager=" + taskManager +
                ", flinkImage=" + flinkImage +
                ", flinkJob=" + flinkJob +
                ", flinkOperator=" + operator +
                ", taskManagers=" + taskManagers +
                '}';
    }
}
