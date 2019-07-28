package com.nextbreakpoint.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Objects;

public class V1FlinkJobSpec {
    @SerializedName("image")
    private String image;
    @SerializedName("className")
    private String className;
    @SerializedName("jarPath")
    private String jarPath;
    @SerializedName("arguments")
    private List<String> arguments;
    @SerializedName("parallelism")
    private Integer parallelism;
    @SerializedName("savepoint")
    private String savepoint;

    public String getImage() {
        return image;
    }

    public V1FlinkJobSpec setImage(String image) {
        this.image = image;
        return this;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public V1FlinkJobSpec setArguments(List<String> arguments) {
        this.arguments = arguments;
        return this;
    }

    public String getClassName() {
        return className;
    }

    public V1FlinkJobSpec setClassName(String className) {
        this.className = className;
        return this;
    }

    public String getJarPath() {
        return jarPath;
    }

    public V1FlinkJobSpec setJarPath(String jarPath) {
        this.jarPath = jarPath;
        return this;
    }

    public String getSavepoint() {
        return savepoint;
    }

    public V1FlinkJobSpec setSavepoint(String savepoint) {
        this.savepoint = savepoint;
        return this;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public V1FlinkJobSpec setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1FlinkJobSpec that = (V1FlinkJobSpec) o;
        return Objects.equals(getImage(), that.getImage()) &&
                Objects.equals(getClassName(), that.getClassName()) &&
                Objects.equals(getJarPath(), that.getJarPath()) &&
                Objects.equals(getArguments(), that.getArguments()) &&
                Objects.equals(getParallelism(), that.getParallelism()) &&
                Objects.equals(getSavepoint(), that.getSavepoint());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getImage(), getClassName(), getJarPath(), getArguments(), getParallelism(), getSavepoint());
    }

    @Override
    public String toString() {
        return "V1FlinkJobSpec{" +
                "image='" + image + '\'' +
                ", className='" + className + '\'' +
                ", jarPath='" + jarPath + '\'' +
                ", arguments=" + arguments +
                ", parallelism=" + parallelism +
                ", savepoint='" + savepoint + '\'' +
                '}';
    }
}
