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
public class V1RestartSpec {
    @SerializedName("restartPolicy")
    private String restartPolicy;
    @SerializedName("restartDelay")
    private Long restartDelay;
    @SerializedName("restartTimeout")
    private Long restartTimeout;
}
