package br.uff.sti.sisiad.consumidor.rad.modelo;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.sql.Clob;
import java.time.LocalDateTime;
@Getter
@Setter
@Table("RADOC.PROV")
public class ProvenanceData {
    @Id
    private Long id;
    private LocalDateTime data_importacao;
    private Clob prov_json;
    private String op;
}
