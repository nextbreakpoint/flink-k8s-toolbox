package com.nextbreakpoint.flink.k8s.crd;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkJobDigest {
    @SerializedName("bootstrap")
    private String bootstrap;
    @SerializedName("savepoint")
    private String savepoint;
    @SerializedName("restart")
    private String restart;
}
