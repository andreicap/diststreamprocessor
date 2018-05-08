package it.polimi.supervisor.worker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Wither;

@Data
@Wither
@NoArgsConstructor
@AllArgsConstructor
public class OperatorDetails {

    private Long operatorId;

    private String host;

    private Integer port;

    private State state;
}
