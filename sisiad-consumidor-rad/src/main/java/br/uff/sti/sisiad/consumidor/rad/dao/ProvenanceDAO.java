package br.uff.sti.sisiad.consumidor.rad.dao;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Repository
public class ProvenanceDAO {
    private final JdbcTemplate jdbcTemplate;

    public ProvenanceDAO(JdbcTemplate jdbcTemplate){
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(Long id, LocalDateTime dataImportacao,String jsonString, String op){
        jdbcTemplate.update("INSERT INTO RADOC.PROV (ID, DATA_IMPORTACAO, PROV_JSON, OP) VALUES (?,?,?,?)",
                id,
                dataImportacao,
                jsonString,
                op
        );
    }


    public Long findFirstByIdAndOpOrderByData_ImportacaoDesc(long id, String op){
        Timestamp timeStamp = jdbcTemplate.queryForObject("SELECT data_importacao FROM RADOC.PROV WHERE id = ? AND op = ? ORDER BY data_importacao DESC FETCH FIRST 1 ROWS ONLY",
                Timestamp.class, id,op);
        if (timeStamp == null) return 0L;
        //else return timeStamp.toLocalDateTime().toEpochSecond(ZoneOffset.UTC);
        else return timeStamp.toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

}
