package br.uff.sti.sisiad.consumidor.rad.util;

import br.uff.sti.sisiad.consumidor.rad.modelo.ExtensaoAtividade;

public interface ProvConstants {

    public static final String MSG_PREF = "message/";
    public static final String IMP_PREF = "importacao/";
    public static final String ATV_PREF = "atividade/";
    public static final String APG_PREF = "aulaPosGraduacao/";
    public static final String ADM_PREF = "administracao/";
    public static final String AFT_PREF = "afastamento/";
    public static final String ESP_PREF = "especial/";
    public static final String OR_PREF = "orientacao/";
    public static final String PROD_PREF = "produto/";
    public static final String INST_PREF = "instituicao/";
    public static final String ORTD_PREF = "orientando/";
    public static final String IDI_PREF = "idioma/";
    public static final String AREA_PREF = "areaConhecimento/";
    public static final String ENC_PREF = "encerramento/";
    public static final String BAN_PREF = "banca/";
    public static final String AUT_PREF = "autorProduto/";

    default String getPrefix(Object entity){
        String prefix = switch (entity.getClass().getSimpleName()) {
            case "AdministracaoRAD" -> ADM_PREF;
            case "AfastamentoRAD" -> AFT_PREF;
            case "AreaConhecimentoRAD" -> AREA_PREF;
            case "AtividadeRAD" -> ATV_PREF;
            case "AulaPosGraduacaoRAD" -> APG_PREF;
            case "AutorProdutoRAD" -> AUT_PREF;
            case "BancaRAD" -> BAN_PREF;
            case "EncerramentoRAD" -> ENC_PREF;
            case "EspecialRAD" -> ESP_PREF;
            case "IdiomaRAD" -> IDI_PREF;
            case "InstituicaoRAD" -> INST_PREF;
            case "OrientacaoRAD" -> OR_PREF;
            case "OrientandoRAD" -> ORTD_PREF;
            case "ProdutoRAD" -> PROD_PREF;
            default -> "";
        };
        return prefix;
    }
}
