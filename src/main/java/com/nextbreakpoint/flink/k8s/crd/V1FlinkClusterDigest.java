package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V1FlinkClusterDigest {
    @SerializedName("runtime")
    private String runtime;
    @SerializedName("jobManager")
    private String jobManager;
    @SerializedName("taskManager")
    private String taskManager;
    @SerializedName("bootstrap")
    private String bootstrap;

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

    public String getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkClusterDigest that = (V1FlinkClusterDigest) o;
        return Objects.equals(getRuntime(), that.getRuntime()) &&
                Objects.equals(getJobManager(), that.getJobManager()) &&
                Objects.equals(getTaskManager(), that.getTaskManager()) &&
                Objects.equals(getBootstrap(), that.getBootstrap());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRuntime(), getJobManager(), getTaskManager(), getBootstrap());
    }

    @Override
    public String toString() {
        return "V1ResourceDigest{" +
                "runtime='" + runtime + '\'' +
                ", jobManager='" + jobManager + '\'' +
                ", taskManager='" + taskManager + '\'' +
                ", bootstrap='" + bootstrap + '\'' +
                '}';
    }
}
