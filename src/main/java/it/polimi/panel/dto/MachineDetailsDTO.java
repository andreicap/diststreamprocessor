package it.polimi.panel.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Wither;

@Data
@Wither
@NoArgsConstructor
@AllArgsConstructor
public class MachineDetailsDTO {

    private Long machineId;

    private String host;

    private String port;
}
