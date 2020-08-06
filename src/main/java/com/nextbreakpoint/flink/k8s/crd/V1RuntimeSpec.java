package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class V1RuntimeSpec {
    @SerializedName("pullSecrets")
    private String pullSecrets;
    @SerializedName("pullPolicy")
    private String pullPolicy;
    @SerializedName("image")
    private String image;

    public String getPullPolicy() {
        return pullPolicy;
    }

    public V1RuntimeSpec setPullPolicy(String pullPolicy) {
        this.pullPolicy = pullPolicy;
        return this;
    }

    public String getPullSecrets() {
        return pullSecrets;
    }

    public V1RuntimeSpec setPullSecrets(String pullSecrets) {
        this.pullSecrets = pullSecrets;
        return this;
    }

    public String getImage() {
        return image;
    }

    public V1RuntimeSpec setImage(String image) {
        this.image = image;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V1RuntimeSpec that = (V1RuntimeSpec) o;
        return Objects.equals(getPullSecrets(), that.getPullSecrets()) &&
                Objects.equals(getPullPolicy(), that.getPullPolicy()) &&
                Objects.equals(getImage(), that.getImage());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPullSecrets(), getPullPolicy(), getImage());
    }

    @Override
    public String toString() {
        return "V1RuntimeSpec{" +
                "pullSecrets='" + pullSecrets + '\'' +
                ", pullPolicy='" + pullPolicy + '\'' +
                ", image='" + image + '\'' +
                '}';
    }
}
