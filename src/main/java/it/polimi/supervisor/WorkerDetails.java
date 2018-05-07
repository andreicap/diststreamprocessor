package it.polimi.supervisor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Wither;

@Data
@Wither
@NoArgsConstructor
@AllArgsConstructor
class WorkerDetails {

    private Long workerId;

    private String host;

    private Integer port;
}
