package com.nextbreakpoint.flinkoperator.common.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V1ResourceDigest {
    @SerializedName("runtime")
    private String runtime;
    @SerializedName("bootstrap")
    private String bootstrap;
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

    public String getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(String bootstrap) {
        this.bootstrap = bootstrap;
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
        V1ResourceDigest that = (V1ResourceDigest) o;
        return Objects.equals(getRuntime(), that.getRuntime()) &&
                Objects.equals(getBootstrap(), that.getBootstrap()) &&
                Objects.equals(getJobManager(), that.getJobManager()) &&
                Objects.equals(getTaskManager(), that.getTaskManager());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRuntime(), getBootstrap(), getJobManager(), getTaskManager());
    }

    @Override
    public String toString() {
        return "V1ResourceDigest{" +
                "runtime='" + runtime + '\'' +
                ", bootstrap='" + bootstrap + '\'' +
                ", jobManager='" + jobManager + '\'' +
                ", taskManager='" + taskManager + '\'' +
                '}';
    }
}
