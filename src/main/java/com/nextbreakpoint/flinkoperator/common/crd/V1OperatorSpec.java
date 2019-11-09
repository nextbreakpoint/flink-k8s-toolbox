package com.nextbreakpoint.flinkoperator.common.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V1OperatorSpec {
    @SerializedName("savepointMode")
    private String savepointMode;
    @SerializedName("savepointPath")
    private String savepointPath;
    @SerializedName("savepointInterval")
    private Long savepointInterval;
    @SerializedName("savepointTargetPath")
    private String savepointTargetPath;
    @SerializedName("jobRestartPolicy")
    private String jobRestartPolicy;
    @SerializedName("taskManagers")
    private Integer taskManagers;

    public String getSavepointMode() {
        return savepointMode;
    }

    public V1OperatorSpec setSavepointMode(String savepointMode) {
        this.savepointMode = savepointMode;
        return this;
    }

    public String getSavepointPath() {
        return savepointPath;
    }

    public V1OperatorSpec setSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
        return this;
    }

    public Long getSavepointInterval() {
        return savepointInterval;
    }

    public V1OperatorSpec setSavepointInterval(Long savepointInterval) {
        this.savepointInterval = savepointInterval;
        return this;
    }

    public String getSavepointTargetPath() {
        return savepointTargetPath;
    }

    public void setSavepointTargetPath(String savepointTargetPath) {
        this.savepointTargetPath = savepointTargetPath;
    }

    public String getJobRestartPolicy() {
        return jobRestartPolicy;
    }

    public void setJobRestartPolicy(String jobRestartPolicy) {
        this.jobRestartPolicy = jobRestartPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1OperatorSpec that = (V1OperatorSpec) o;
        return Objects.equals(getSavepointMode(), that.getSavepointMode()) &&
                Objects.equals(getSavepointPath(), that.getSavepointPath()) &&
                Objects.equals(getSavepointInterval(), that.getSavepointInterval()) &&
                Objects.equals(getSavepointTargetPath(), that.getSavepointTargetPath()) &&
                Objects.equals(getJobRestartPolicy(), that.getJobRestartPolicy());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSavepointMode(), getSavepointPath(), getSavepointInterval(), getSavepointTargetPath(), getJobRestartPolicy());
    }

    @Override
    public String toString() {
        return "V1FlinkOperatorSpec{" +
                "savepointMode='" + savepointMode + '\'' +
                ", savepointPath='" + savepointPath + '\'' +
                ", savepointInterval=" + savepointInterval +
                ", savepointTargetPath='" + savepointTargetPath + '\'' +
                ", jobRestartPolicy='" + jobRestartPolicy + '\'' +
                '}';
    }
}
