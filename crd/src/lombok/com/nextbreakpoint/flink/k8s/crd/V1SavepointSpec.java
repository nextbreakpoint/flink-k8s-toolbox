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
public class V1SavepointSpec {
    @SerializedName("savepointMode")
    private String savepointMode;
    @SerializedName("savepointPath")
    private String savepointPath;
    @SerializedName("savepointInterval")
    private Long savepointInterval;
    @SerializedName("savepointTargetPath")
    private String savepointTargetPath;
}
