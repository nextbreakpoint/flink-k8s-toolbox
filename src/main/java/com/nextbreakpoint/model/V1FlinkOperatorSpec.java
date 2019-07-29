package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Objects;

public class V1FlinkOperatorSpec {
    @SerializedName("savepoint")
    private String savepoint;
    @SerializedName("targetPath")
    private String targetPath;

    public String getSavepoint() {
        return savepoint;
    }

    public V1FlinkOperatorSpec setSavepoint(String savepoint) {
        this.savepoint = savepoint;
        return this;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkOperatorSpec that = (V1FlinkOperatorSpec) o;
        return Objects.equals(getTargetPath(), that.getTargetPath()) &&
                Objects.equals(getSavepoint(), that.getSavepoint());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSavepoint(), getTargetPath());
    }

    @Override
    public String toString() {
        return "V1FlinkOperatorSpec{" +
                ", savepoint='" + savepoint +
                ", targetPath=" + targetPath + '\'' +
                '}';
    }
}
