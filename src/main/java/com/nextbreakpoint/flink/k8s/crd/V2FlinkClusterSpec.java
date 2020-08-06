package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Objects;

public class V2FlinkClusterSpec {
    @SerializedName("runtime")
    private V1RuntimeSpec runtime;
    @SerializedName("jobManager")
    private V1JobManagerSpec jobManager;
    @SerializedName("taskManager")
    private V1TaskManagerSpec taskManager;
    @SerializedName("supervisor")
    private V1SupervisorSpec supervisor;
    @SerializedName("taskManagers")
    private Integer taskManagers;
    @SerializedName("jobs")
    private List<V2FlinkClusterJobSpec> jobs;
    @SerializedName("rescaleDelay")
    private Integer rescaleDelay;

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

    public V1SupervisorSpec getSupervisor() {
        return supervisor;
    }

    public void setSupervisor(V1SupervisorSpec supervisor) {
        this.supervisor = supervisor;
    }

    public Integer getTaskManagers() {
        return taskManagers;
    }

    public void setTaskManagers(Integer taskManagers) {
        this.taskManagers = taskManagers;
    }

    public List<V2FlinkClusterJobSpec> getJobs() {
        return jobs;
    }

    public void setJobs(List<V2FlinkClusterJobSpec> jobs) {
        this.jobs = jobs;
    }

    public Integer getRescaleDelay() {
        return rescaleDelay;
    }

    public void setRescaleDelay(Integer rescaleDelay) {
        this.rescaleDelay = rescaleDelay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V2FlinkClusterSpec that = (V2FlinkClusterSpec) o;
        return Objects.equals(getRuntime(), that.getRuntime()) &&
                Objects.equals(getJobManager(), that.getJobManager()) &&
                Objects.equals(getTaskManager(), that.getTaskManager()) &&
                Objects.equals(getSupervisor(), that.getSupervisor()) &&
                Objects.equals(getTaskManagers(), that.getTaskManagers()) &&
                Objects.equals(getJobs(), that.getJobs()) &&
                Objects.equals(getRescaleDelay(), that.getRescaleDelay());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRuntime(), getJobManager(), getTaskManager(), getSupervisor(), getTaskManagers(), getJobs(), getRescaleDelay());
    }

    @Override
    public String toString() {
        return "V2FlinkClusterSpec{" +
                "runtime=" + runtime +
                ", jobManager=" + jobManager +
                ", taskManager=" + taskManager +
                ", supervisor=" + supervisor +
                ", taskManagers=" + taskManagers +
                ", jobs=" + jobs +
                ", rescaleDelay=" + rescaleDelay +
                '}';
    }
}
