package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;

import java.util.List;
import java.util.Objects;

public class V1BootstrapSpec {
    @SerializedName("pullSecrets")
    private String pullSecrets;
    @SerializedName("pullPolicy")
    private String pullPolicy;
    @SerializedName("image")
    private String image;
    @SerializedName("className")
    private String className;
    @SerializedName("jarPath")
    private String jarPath;
    @SerializedName("arguments")
    private List<String> arguments;
    @SerializedName("serviceAccount")
    private String serviceAccount;
    @SerializedName("resources")
    private V1ResourceRequirements resources;

    public String getPullSecrets() {
        return pullSecrets;
    }

    public void setPullSecrets(String pullSecrets) {
        this.pullSecrets = pullSecrets;
    }

    public String getPullPolicy() {
        return pullPolicy;
    }

    public void setPullPolicy(String pullPolicy) {
        this.pullPolicy = pullPolicy;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public void setArguments(List<String> arguments) {
        this.arguments = arguments;
    }

    public String getServiceAccount() {
        return serviceAccount;
    }

    public void setServiceAccount(String serviceAccount) {
        this.serviceAccount = serviceAccount;
    }

    public V1ResourceRequirements getResources() {
        return resources;
    }

    public void setResources(V1ResourceRequirements resources) {
        this.resources = resources;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1BootstrapSpec that = (V1BootstrapSpec) o;
        return Objects.equals(getPullSecrets(), that.getPullSecrets()) &&
                Objects.equals(getPullPolicy(), that.getPullPolicy()) &&
                Objects.equals(getImage(), that.getImage()) &&
                Objects.equals(getClassName(), that.getClassName()) &&
                Objects.equals(getJarPath(), that.getJarPath()) &&
                Objects.equals(getArguments(), that.getArguments()) &&
                Objects.equals(getServiceAccount(), that.getServiceAccount()) &&
                Objects.equals(getResources(), that.getResources());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPullSecrets(), getPullPolicy(), getImage(), getClassName(), getJarPath(), getArguments(), getServiceAccount(), getResources());
    }

    @Override
    public String toString() {
        return "V1BootstrapSpec{" +
                "pullSecrets='" + pullSecrets + '\'' +
                ", pullPolicy='" + pullPolicy + '\'' +
                ", image='" + image + '\'' +
                ", className='" + className + '\'' +
                ", jarPath='" + jarPath + '\'' +
                ", arguments=" + arguments +
                ", serviceAccount='" + serviceAccount + '\'' +
                ", resources=" + resources +
                '}';
    }
}
