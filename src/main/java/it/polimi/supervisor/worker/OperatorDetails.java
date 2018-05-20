package it.polimi.supervisor.worker;

import it.polimi.supervisor.graph.Aggregation;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Wither;

@Data
@Wither
@NoArgsConstructor
@AllArgsConstructor
public class OperatorDetails {

    private Long registrationId;

    private String host;

    private Integer port;

    private State state;

    private Integer operatorId;

    private Long windowSize;

    private Long windowSlide;

    private Aggregation aggregation;
}
