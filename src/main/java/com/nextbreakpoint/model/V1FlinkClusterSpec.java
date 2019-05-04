package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.models.V1EnvVar;

import java.util.List;
import java.util.Objects;

public class V1FlinkClusterSpec {
    @SerializedName("pullSecrets")
    private String pullSecrets;
    @SerializedName("pullPolicy")
    private String pullPolicy;
    @SerializedName("flinkImage")
    private String flinkImage;
    @SerializedName("serviceAccount")
    private String serviceAccount;
    @SerializedName("jobmanagerCPUs")
    private Float jobmanagerCPUs;
    @SerializedName("jobmanagerMemory")
    private Integer jobmanagerMemory;
    @SerializedName("jobmanagerStorageSize")
    private Integer jobmanagerStorageSize;
    @SerializedName("jobmanagerStorageClass")
    private String jobmanagerStorageClass;
    @SerializedName("jobmanagerEnvironment")
    private List<V1EnvVar> jobmanagerEnvironment;
    @SerializedName("jobmanagerServiceMode")
    private String jobmanagerServiceMode;
    @SerializedName("taskmanagerCPUs")
    private Float taskmanagerCPUs;
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
    @SerializedName("taskmanagerEnvironment")
    private List<V1EnvVar> taskmanagerEnvironment;
    @SerializedName("jobImage")
    private String jobImage;
    @SerializedName("jobClassName")
    private String jobClassName;
    @SerializedName("jobJarPath")
    private String jobJarPath;
    @SerializedName("jobArguments")
    private List<String> jobArguments;
    @SerializedName("jobParallelism")
    private Integer jobParallelism;
    @SerializedName("jobSavepoint")
    private String jobSavepoint;

    public String getPullPolicy() {
        return pullPolicy;
    }

    public V1FlinkClusterSpec setPullPolicy(String pullPolicy) {
        this.pullPolicy = pullPolicy;
        return this;
    }

    public String getJobmanagerServiceMode() {
        return jobmanagerServiceMode;
    }

    public V1FlinkClusterSpec setJobmanagerServiceMode(String jobmanagerServiceMode) {
        this.jobmanagerServiceMode = jobmanagerServiceMode;
        return this;
    }

    public Float getJobmanagerCPUs() {
        return jobmanagerCPUs;
    }

    public V1FlinkClusterSpec setJobmanagerCPUs(Float jobmanagerCPUs) {
        this.jobmanagerCPUs = jobmanagerCPUs;
        return this;
    }

    public Integer getJobmanagerMemory() {
        return jobmanagerMemory;
    }

    public V1FlinkClusterSpec setJobmanagerMemory(Integer jobmanagerMemory) {
        this.jobmanagerMemory = jobmanagerMemory;
        return this;
    }

    public Integer getJobmanagerStorageSize() {
        return jobmanagerStorageSize;
    }

    public V1FlinkClusterSpec setJobmanagerStorageSize(Integer jobmanagerStorageSize) {
        this.jobmanagerStorageSize = jobmanagerStorageSize;
        return this;
    }

    public String getJobmanagerStorageClass() {
        return jobmanagerStorageClass;
    }

    public V1FlinkClusterSpec setJobmanagerStorageClass(String jobmanagerStorageClass) {
        this.jobmanagerStorageClass = jobmanagerStorageClass;
        return this;
    }

    public Float getTaskmanagerCPUs() {
        return taskmanagerCPUs;
    }

    public V1FlinkClusterSpec setTaskmanagerCPUs(Float taskmanagerCPUs) {
        this.taskmanagerCPUs = taskmanagerCPUs;
        return this;
    }

    public Integer getTaskmanagerMemory() {
        return taskmanagerMemory;
    }

    public V1FlinkClusterSpec setTaskmanagerMemory(Integer taskmanagerMemory) {
        this.taskmanagerMemory = taskmanagerMemory;
        return this;
    }

    public Integer getTaskmanagerStorageSize() {
        return taskmanagerStorageSize;
    }

    public V1FlinkClusterSpec setTaskmanagerStorageSize(Integer taskmanagerStorageSize) {
        this.taskmanagerStorageSize = taskmanagerStorageSize;
        return this;
    }

    public String getTaskmanagerStorageClass() {
        return taskmanagerStorageClass;
    }

    public V1FlinkClusterSpec setTaskmanagerStorageClass(String taskmanagerStorageClass) {
        this.taskmanagerStorageClass = taskmanagerStorageClass;
        return this;
    }

    public Integer getTaskmanagerReplicas() {
        return taskmanagerReplicas;
    }

    public V1FlinkClusterSpec setTaskmanagerReplicas(Integer taskmanagerReplicas) {
        this.taskmanagerReplicas = taskmanagerReplicas;
        return this;
    }

    public Integer getTaskmanagerTaskSlots() {
        return taskmanagerTaskSlots;
    }

    public V1FlinkClusterSpec setTaskmanagerTaskSlots(Integer taskmanagerTaskSlots) {
        this.taskmanagerTaskSlots = taskmanagerTaskSlots;
        return this;
    }

    public String getPullSecrets() {
        return pullSecrets;
    }

    public V1FlinkClusterSpec setPullSecrets(String pullSecrets) {
        this.pullSecrets = pullSecrets;
        return this;
    }

    public String getFlinkImage() {
        return flinkImage;
    }

    public V1FlinkClusterSpec setFlinkImage(String flinkImage) {
        this.flinkImage = flinkImage;
        return this;
    }

    public String getJobImage() {
        return jobImage;
    }

    public V1FlinkClusterSpec setJobImage(String jobImage) {
        this.jobImage = jobImage;
        return this;
    }

    public List<String> getJobArguments() {
        return jobArguments;
    }

    public V1FlinkClusterSpec setJobArguments(List<String> jobArguments) {
        this.jobArguments = jobArguments;
        return this;
    }

    public String getServiceAccount() {
        return serviceAccount;
    }

    public V1FlinkClusterSpec setServiceAccount(String serviceAccount) {
        this.serviceAccount = serviceAccount;
        return this;
    }

    public String getJobClassName() {
        return jobClassName;
    }

    public V1FlinkClusterSpec setJobClassName(String jobClassName) {
        this.jobClassName = jobClassName;
        return this;
    }

    public String getJobJarPath() {
        return jobJarPath;
    }

    public V1FlinkClusterSpec setJobJarPath(String jobJarPath) {
        this.jobJarPath = jobJarPath;
        return this;
    }

    public String getJobSavepoint() {
        return jobSavepoint;
    }

    public V1FlinkClusterSpec setJobSavepoint(String jobSavepoint) {
        this.jobSavepoint = jobSavepoint;
        return this;
    }

    public Integer getJobParallelism() {
        return jobParallelism;
    }

    public V1FlinkClusterSpec setJobParallelism(Integer jobParallelism) {
        this.jobParallelism = jobParallelism;
        return this;
    }

    public List<V1EnvVar> getJobmanagerEnvironment() {
        return jobmanagerEnvironment;
    }

    public V1FlinkClusterSpec setJobmanagerEnvironment(List<V1EnvVar> jobmanagerEnvironment) {
        this.jobmanagerEnvironment = jobmanagerEnvironment;
        return this;
    }

    public List<V1EnvVar> getTaskmanagerEnvironment() {
        return taskmanagerEnvironment;
    }

    public V1FlinkClusterSpec setTaskmanagerEnvironment(List<V1EnvVar> taskmanagerEnvironment) {
        this.taskmanagerEnvironment = taskmanagerEnvironment;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkClusterSpec that = (V1FlinkClusterSpec) o;
        return Objects.equals(getPullSecrets(), that.getPullSecrets()) &&
                Objects.equals(getPullPolicy(), that.getPullPolicy()) &&
                Objects.equals(getFlinkImage(), that.getFlinkImage()) &&
                Objects.equals(getServiceAccount(), that.getServiceAccount()) &&
                Objects.equals(getJobmanagerCPUs(), that.getJobmanagerCPUs()) &&
                Objects.equals(getJobmanagerMemory(), that.getJobmanagerMemory()) &&
                Objects.equals(getJobmanagerStorageSize(), that.getJobmanagerStorageSize()) &&
                Objects.equals(getJobmanagerStorageClass(), that.getJobmanagerStorageClass()) &&
                Objects.equals(getJobmanagerEnvironment(), that.getJobmanagerEnvironment()) &&
                Objects.equals(getJobmanagerServiceMode(), that.getJobmanagerServiceMode()) &&
                Objects.equals(getTaskmanagerCPUs(), that.getTaskmanagerCPUs()) &&
                Objects.equals(getTaskmanagerMemory(), that.getTaskmanagerMemory()) &&
                Objects.equals(getTaskmanagerStorageSize(), that.getTaskmanagerStorageSize()) &&
                Objects.equals(getTaskmanagerStorageClass(), that.getTaskmanagerStorageClass()) &&
                Objects.equals(getTaskmanagerReplicas(), that.getTaskmanagerReplicas()) &&
                Objects.equals(getTaskmanagerTaskSlots(), that.getTaskmanagerTaskSlots()) &&
                Objects.equals(getTaskmanagerEnvironment(), that.getTaskmanagerEnvironment()) &&
                Objects.equals(getJobImage(), that.getJobImage()) &&
                Objects.equals(getJobClassName(), that.getJobClassName()) &&
                Objects.equals(getJobJarPath(), that.getJobJarPath()) &&
                Objects.equals(getJobArguments(), that.getJobArguments()) &&
                Objects.equals(getJobParallelism(), that.getJobParallelism()) &&
                Objects.equals(getJobSavepoint(), that.getJobSavepoint());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPullSecrets(), getPullPolicy(), getFlinkImage(), getServiceAccount(), getJobmanagerCPUs(), getJobmanagerMemory(), getJobmanagerStorageSize(), getJobmanagerStorageClass(), getJobmanagerEnvironment(), getJobmanagerServiceMode(), getTaskmanagerCPUs(), getTaskmanagerMemory(), getTaskmanagerStorageSize(), getTaskmanagerStorageClass(), getTaskmanagerReplicas(), getTaskmanagerTaskSlots(), getTaskmanagerEnvironment(), getJobImage(), getJobClassName(), getJobJarPath(), getJobArguments(), getJobParallelism(), getJobSavepoint());
    }

    @Override
    public String toString() {
        return "V1FlinkClusterSpec{" +
                "pullSecrets='" + pullSecrets + '\'' +
                ", pullPolicy='" + pullPolicy + '\'' +
                ", flinkImage='" + flinkImage + '\'' +
                ", serviceAccount='" + serviceAccount + '\'' +
                ", jobmanagerCPUs=" + jobmanagerCPUs +
                ", jobmanagerMemory=" + jobmanagerMemory +
                ", jobmanagerStorageSize=" + jobmanagerStorageSize +
                ", jobmanagerStorageClass='" + jobmanagerStorageClass + '\'' +
                ", jobmanagerEnvironment=" + jobmanagerEnvironment +
                ", jobmanagerServiceMode='" + jobmanagerServiceMode + '\'' +
                ", taskmanagerCPUs=" + taskmanagerCPUs +
                ", taskmanagerMemory=" + taskmanagerMemory +
                ", taskmanagerStorageSize=" + taskmanagerStorageSize +
                ", taskmanagerStorageClass='" + taskmanagerStorageClass + '\'' +
                ", taskmanagerReplicas=" + taskmanagerReplicas +
                ", taskmanagerTaskSlots=" + taskmanagerTaskSlots +
                ", taskmanagerEnvironment=" + taskmanagerEnvironment +
                ", jobImage='" + jobImage + '\'' +
                ", jobClassName='" + jobClassName + '\'' +
                ", jobJarPath='" + jobJarPath + '\'' +
                ", jobArguments=" + jobArguments +
                ", jobParallelism=" + jobParallelism +
                ", jobSavepoint='" + jobSavepoint + '\'' +
                '}';
    }
}
