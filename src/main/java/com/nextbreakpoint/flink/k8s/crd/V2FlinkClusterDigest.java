package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V2FlinkClusterDigest {
    @SerializedName("runtime")
    private String runtime;
    @SerializedName("jobManager")
    private String jobManager;
    @SerializedName("taskManager")
    private String taskManager;

    public String getRuntime() {
        return runtime;
    }

    public void setRuntime(String runtime) {
        this.runtime = runtime;
    }

    public String getJobManager() {
        return jobManager;
    }

    public void setJobManager(String jobManager) {
        this.jobManager = jobManager;
    }

    public String getTaskManager() {
        return taskManager;
    }

    public void setTaskManager(String taskManager) {
        this.taskManager = taskManager;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V2FlinkClusterDigest that = (V2FlinkClusterDigest) o;
        return Objects.equals(getRuntime(), that.getRuntime()) &&
                Objects.equals(getJobManager(), that.getJobManager()) &&
                Objects.equals(getTaskManager(), that.getTaskManager());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRuntime(), getJobManager(), getTaskManager());
    }

    @Override
    public String toString() {
        return "V2FlinkClusterDigest{" +
                "runtime='" + runtime + '\'' +
                ", jobManager='" + jobManager + '\'' +
                ", taskManager='" + taskManager + '\'' +
                '}';
    }
}
