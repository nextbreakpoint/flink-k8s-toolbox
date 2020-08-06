package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V1FlinkClusterSpec {
    @SerializedName("runtime")
    private V1RuntimeSpec runtime;
    @SerializedName("jobManager")
    private V1JobManagerSpec jobManager;
    @SerializedName("taskManager")
    private V1TaskManagerSpec taskManager;
    @SerializedName("bootstrap")
    private V1BootstrapSpec bootstrap;
    @SerializedName("operator")
    private V1OperatorSpec operator;
    @SerializedName("taskManagers")
    private Integer taskManagers;

    public V1RuntimeSpec getRuntime() {
        return runtime;
    }

    public void setRuntime(V1RuntimeSpec runtime) {
        this.runtime = runtime;
    }

    public V1JobManagerSpec getJobManager() {
        return jobManager;
    }

    public void setJobManager(V1JobManagerSpec jobManager) {
        this.jobManager = jobManager;
    }

    public V1TaskManagerSpec getTaskManager() {
        return taskManager;
    }

    public void setTaskManager(V1TaskManagerSpec taskManager) {
        this.taskManager = taskManager;
    }

    public V1BootstrapSpec getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(V1BootstrapSpec bootstrap) {
        this.bootstrap = bootstrap;
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
        return Objects.equals(getRuntime(), that.getRuntime()) &&
                Objects.equals(getJobManager(), that.getJobManager()) &&
                Objects.equals(getTaskManager(), that.getTaskManager()) &&
                Objects.equals(getBootstrap(), that.getBootstrap()) &&
                Objects.equals(getOperator(), that.getOperator()) &&
                Objects.equals(getTaskManagers(), that.getTaskManagers());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRuntime(), getJobManager(), getTaskManager(), getBootstrap(), getOperator(), getTaskManagers());
    }

    @Override
    public String toString() {
        return "V1FlinkClusterSpec{" +
                "runtime=" + runtime +
                ", jobManager=" + jobManager +
                ", taskManager=" + taskManager +
                ", bootstrap=" + bootstrap +
                ", operator=" + operator +
                ", taskManagers=" + taskManagers +
                '}';
    }
}
