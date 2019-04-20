package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Objects;

public class V1FlinkClusterSpec {
    @SerializedName("clusterName")
    private String clusterName;
    @SerializedName("environment")
    private String environment;
    @SerializedName("pullSecrets")
    private String pullSecrets;
    @SerializedName("pullPolicy")
    private String pullPolicy;
    @SerializedName("flinkImage")
    private String flinkImage;
    @SerializedName("serviceMode")
    private String serviceMode;
    @SerializedName("jobmanagerCpus")
    private Float jobmanagerCpus;
    @SerializedName("jobmanagerMemory")
    private Integer jobmanagerMemory;
    @SerializedName("jobmanagerStorageSize")
    private Integer jobmanagerStorageSize;
    @SerializedName("jobmanagerStorageClass")
    private String jobmanagerStorageClass;
    @SerializedName("taskmanagerCpus")
    private Float taskmanagerCpus;
    @SerializedName("taskmanagerMemory")
    private Integer taskmanagerMemory;
    @SerializedName("taskmanagerStorageSize")
    private Integer taskmanagerStorageSize;
    @SerializedName("taskmanagerStorageClass")
    private String taskmanagerStorageClass;
    @SerializedName("taskmanagerReplicas")
    private Integer taskmanagerReplicas;
    @SerializedName("taskmanagerTaskSlots")
    private Integer taskmanagerTaskSlots;
    @SerializedName("sidecarImage")
    private String sidecarImage;
    @SerializedName("sidecarArguments")
    private List<String> sidecarArguments;

    public String getPullPolicy() {
        return pullPolicy;
    }

    public void setPullPolicy(String pullPolicy) {
        this.pullPolicy = pullPolicy;
    }

    public String getServiceMode() {
        return serviceMode;
    }

    public void setServiceMode(String serviceMode) {
        this.serviceMode = serviceMode;
    }

    public Float getJobmanagerCpus() {
        return jobmanagerCpus;
    }

    public void setJobmanagerCpus(Float jobmanagerCpus) {
        this.jobmanagerCpus = jobmanagerCpus;
    }

    public Integer getJobmanagerMemory() {
        return jobmanagerMemory;
    }

    public void setJobmanagerMemory(Integer jobmanagerMemory) {
        this.jobmanagerMemory = jobmanagerMemory;
    }

    public Integer getJobmanagerStorageSize() {
        return jobmanagerStorageSize;
    }

    public void setJobmanagerStorageSize(Integer jobmanagerStorageSize) {
        this.jobmanagerStorageSize = jobmanagerStorageSize;
    }

    public String getJobmanagerStorageClass() {
        return jobmanagerStorageClass;
    }

    public void setJobmanagerStorageClass(String jobmanagerStorageClass) {
        this.jobmanagerStorageClass = jobmanagerStorageClass;
    }

    public Float getTaskmanagerCpus() {
        return taskmanagerCpus;
    }

    public void setTaskmanagerCpus(Float taskmanagerCpus) {
        this.taskmanagerCpus = taskmanagerCpus;
    }

    public Integer getTaskmanagerMemory() {
        return taskmanagerMemory;
    }

    public void setTaskmanagerMemory(Integer taskmanagerMemory) {
        this.taskmanagerMemory = taskmanagerMemory;
    }

    public Integer getTaskmanagerStorageSize() {
        return taskmanagerStorageSize;
    }

    public void setTaskmanagerStorageSize(Integer taskmanagerStorageSize) {
        this.taskmanagerStorageSize = taskmanagerStorageSize;
    }

    public String getTaskmanagerStorageClass() {
        return taskmanagerStorageClass;
    }

    public void setTaskmanagerStorageClass(String taskmanagerStorageClass) {
        this.taskmanagerStorageClass = taskmanagerStorageClass;
    }

    public Integer getTaskmanagerReplicas() {
        return taskmanagerReplicas;
    }

    public void setTaskmanagerReplicas(Integer taskmanagerReplicas) {
        this.taskmanagerReplicas = taskmanagerReplicas;
    }

    public Integer getTaskmanagerTaskSlots() {
        return taskmanagerTaskSlots;
    }

    public void setTaskmanagerTaskSlots(Integer taskmanagerTaskSlots) {
        this.taskmanagerTaskSlots = taskmanagerTaskSlots;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getPullSecrets() {
        return pullSecrets;
    }

    public void setPullSecrets(String pullSecrets) {
        this.pullSecrets = pullSecrets;
    }

    public String getFlinkImage() {
        return flinkImage;
    }

    public void setFlinkImage(String flinkImage) {
        this.flinkImage = flinkImage;
    }

    public String getSidecarImage() {
        return sidecarImage;
    }

    public void setSidecarImage(String sidecarImage) {
        this.sidecarImage = sidecarImage;
    }

    public List<String> getSidecarArguments() {
        return sidecarArguments;
    }

    public void setSidecarArguments(List<String> sidecarArguments) {
        this.sidecarArguments = sidecarArguments;
    }

    @Override
    public String toString() {
        return "class V1FlinkClusterSpec {" +
                "clusterName='" + clusterName + '\'' +
                ", environment='" + environment + '\'' +
                ", pullSecrets='" + pullSecrets + '\'' +
                ", pullPolicy='" + pullPolicy + '\'' +
                ", flinkImage='" + flinkImage + '\'' +
                ", serviceMode='" + serviceMode + '\'' +
                ", jobmanagerCpus=" + jobmanagerCpus +
                ", jobmanagerMemory=" + jobmanagerMemory +
                ", jobmanagerStorageSize=" + jobmanagerStorageSize +
                ", jobmanagerStorageClass='" + jobmanagerStorageClass + '\'' +
                ", taskmanagerCpus=" + taskmanagerCpus +
                ", taskmanagerMemory=" + taskmanagerMemory +
                ", taskmanagerStorageSize=" + taskmanagerStorageSize +
                ", taskmanagerStorageClass='" + taskmanagerStorageClass + '\'' +
                ", taskmanagerReplicas=" + taskmanagerReplicas +
                ", taskmanagerTaskSlots=" + taskmanagerTaskSlots +
                ", sidecarImage='" + sidecarImage + '\'' +
                ", sidecarArguments=" + sidecarArguments +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkClusterSpec that = (V1FlinkClusterSpec) o;
        return Objects.equals(clusterName, that.clusterName) &&
                Objects.equals(environment, that.environment) &&
                Objects.equals(pullSecrets, that.pullSecrets) &&
                Objects.equals(pullPolicy, that.pullPolicy) &&
                Objects.equals(flinkImage, that.flinkImage) &&
                Objects.equals(serviceMode, that.serviceMode) &&
                Objects.equals(jobmanagerCpus, that.jobmanagerCpus) &&
                Objects.equals(jobmanagerMemory, that.jobmanagerMemory) &&
                Objects.equals(jobmanagerStorageSize, that.jobmanagerStorageSize) &&
                Objects.equals(jobmanagerStorageClass, that.jobmanagerStorageClass) &&
                Objects.equals(taskmanagerCpus, that.taskmanagerCpus) &&
                Objects.equals(taskmanagerMemory, that.taskmanagerMemory) &&
                Objects.equals(taskmanagerStorageSize, that.taskmanagerStorageSize) &&
                Objects.equals(taskmanagerStorageClass, that.taskmanagerStorageClass) &&
                Objects.equals(taskmanagerReplicas, that.taskmanagerReplicas) &&
                Objects.equals(taskmanagerTaskSlots, that.taskmanagerTaskSlots) &&
                Objects.equals(sidecarImage, that.sidecarImage) &&
                Objects.equals(sidecarArguments, that.sidecarArguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterName, environment, pullSecrets, pullPolicy, flinkImage, serviceMode, jobmanagerCpus, jobmanagerMemory, jobmanagerStorageSize, jobmanagerStorageClass, taskmanagerCpus, taskmanagerMemory, taskmanagerStorageSize, taskmanagerStorageClass, taskmanagerReplicas, taskmanagerTaskSlots, sidecarImage, sidecarArguments);
    }
}
