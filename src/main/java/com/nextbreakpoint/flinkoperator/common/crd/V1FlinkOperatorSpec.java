package com.nextbreakpoint.flinkoperator.common.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V1FlinkOperatorSpec {
    @SerializedName("savepointMode")
    private String savepointMode;
    @SerializedName("savepointPath")
    private String savepointPath;
    @SerializedName("savepointInterval")
    private Long savepointInterval;
    @SerializedName("savepointTargetPath")
    private String savepointTargetPath;

    public String getSavepointMode() {
        return savepointMode;
    }

    public V1FlinkOperatorSpec setSavepointMode(String savepointMode) {
        this.savepointMode = savepointMode;
        return this;
    }

    public String getSavepointPath() {
        return savepointPath;
    }

    public V1FlinkOperatorSpec setSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
        return this;
    }

    public Long getSavepointInterval() {
        return savepointInterval;
    }

    public V1FlinkOperatorSpec setSavepointInterval(Long savepointInterval) {
        this.savepointInterval = savepointInterval;
        return this;
    }

    public String getSavepointTargetPath() {
        return savepointTargetPath;
    }

    public void setSavepointTargetPath(String savepointTargetPath) {
        this.savepointTargetPath = savepointTargetPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkOperatorSpec that = (V1FlinkOperatorSpec) o;
        return Objects.equals(getSavepointMode(), that.getSavepointMode()) &&
                Objects.equals(getSavepointPath(), that.getSavepointPath()) &&
                Objects.equals(getSavepointInterval(), that.getSavepointInterval()) &&
                Objects.equals(getSavepointTargetPath(), that.getSavepointTargetPath());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSavepointPath(), getSavepointMode(), getSavepointInterval(), getSavepointTargetPath());
    }

    @Override
    public String toString() {
        return "V1FlinkOperatorSpec{" +
                ", savepointMode='" + savepointMode +
                ", savepointPath='" + savepointPath +
                ", savepointInterval='" + savepointInterval +
                ", savepointTargetPath=" + savepointTargetPath + '\'' +
                '}';
    }
}
