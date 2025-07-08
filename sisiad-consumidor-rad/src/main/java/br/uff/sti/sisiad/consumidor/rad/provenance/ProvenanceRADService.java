package br.uff.sti.sisiad.consumidor.rad.provenance;

import br.uff.sti.sisiad.consumidor.rad.dao.ProvenanceDAO;
import br.uff.sti.sisiad.consumidor.rad.modelo.*;
import br.uff.sti.sisiad.consumidor.rad.modelo.atividade.AtividadeRAD;
import br.uff.sti.sisiad.consumidor.rad.modelo.atividade.OrientandoRAD;
import br.uff.sti.sisiad.consumidor.rad.modelo.atividade.especial.BancaRAD;
import br.uff.sti.sisiad.consumidor.rad.util.ProvConstants;
import br.uff.sti.sisiad.modelo.importacao.Importacao;
import lombok.Getter;
import org.openprovenance.prov.model.*;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.time.ZoneOffset;
import java.util.*;

@Scope(value="prototype", proxyMode = ScopedProxyMode.TARGET_CLASS)
@Service
public class ProvenanceRADService extends ExtensaoProvenanceService implements ProvConstants {

    private final ProvenanceDAO provenanceDAO;
    private final HashMap<String, Statement> statements = new HashMap<String, Statement>();

    @Getter
    private final Set<Object> objetosSecundarios = new HashSet<>();

    public ProvenanceRADService(ProvenanceDAO provenanceDAO) {
        super("rad", "https://app.uff.br/rad#");
        this.provenanceDAO = provenanceDAO;
    }

    public Entity buildImportacaoRADEntity(ImportacaoRAD importacaoRAD, Long epochDataImportacao) {
        Map<String,String> values = new HashMap<>();
        values.put("epochDataImportacao",epochDataImportacao.toString());
        values.put("importacaoID",importacaoRAD.getId().toString());
        values.putAll(getFieldsExceptPublic2(importacaoRAD));
        return newEntityExtended(IMP_PREF + importacaoRAD.getId() + "/" + epochDataImportacao, importacaoRAD.getClass().getAnnotation(Table.class).value(), "RECORD", values);
    }

    public Entity buildAtividadeEntity(AtividadeRAD atividadeRAD, Long epochDataImportacao) {
        return newEntityExtended(ATV_PREF + atividadeRAD.getId() + "/" + epochDataImportacao, atividadeRAD.getClass().getAnnotation(Table.class).value(), "RECORD", getFieldsExceptPublic2(atividadeRAD));
    }

    public Entity buildProdutoEntity(ProdutoRAD produtoRAD, Long epochDataImportacao) {
        return newEntityExtended(PROD_PREF + produtoRAD.getId()+ "/" + epochDataImportacao, produtoRAD.getClass().getAnnotation(Table.class).value(), "RECORD", getFieldsExceptPublic2(produtoRAD));
    }

    public Entity buildExtensaoEntity(ExtensaoAtividade extensaoAtividade, Long epochDataImportacao, List<String> listCamposIgnorados) {
        Map<String, String> values = getFieldsExceptPublic(extensaoAtividade);
        values.put("ignoredFields",CollectionUtils.isEmpty(listCamposIgnorados) ? "null" : listCamposIgnorados.toString());
        return newEntityExtended(getPrefix(extensaoAtividade) + extensaoAtividade.getId() + "/" + epochDataImportacao, extensaoAtividade.getClass().getAnnotation(Table.class).value(), "RECORD", values);
    }

    public Entity buildGeneralEntity(Object obj, Long epochDataImportacao) {
        Entity objEntity = null;
        switch (obj.getClass().getSimpleName()) {
            case "InstituicaoRAD":
                objEntity = newEntityExtended(getPrefix(obj) + ((InstituicaoRAD) obj).getId() +
                        "/" + epochDataImportacao, obj.getClass().getAnnotation(Table.class).value(),
                        "RECORD", getFieldsExceptPublic2(obj));
                break;
            case "OrientandoRAD":
            case "AutorProdutoRAD":
            case "AreaConhecimentoRAD":
                objEntity = newEntityExtended(getPrefix(obj) + Objects.hashCode(obj) +
                        "/" + epochDataImportacao,
                        obj.getClass().getAnnotation(Table.class).value(),
                        "RECORD", getFieldsExceptPublic2(obj));
                break;
            case "BancaRAD":
            case "IdiomaRAD":
                objEntity = newEntityExtended(getPrefix(obj) + epochDataImportacao,
                        obj.getClass().getAnnotation(Table.class).value(),
                        "RECORD", getFieldsExceptPublic2(obj));
                break;
        }
        return objEntity;
    }

    public void buildMentionOf(Entity specific, String nameGeneral, String nameBundle) {
        MentionOf mentionOf = pFactory.newMentionOf(specific.getId(), qName(nameGeneral), qName(nameBundle));
        statements.put("mentionOf", mentionOf);
    }

    /**
     * Constrói Bundle para importação do tipo UPSERT
     * ATIVIDADE
     */
    public Document buildBundle(ImportacaoRAD importacaoRAD, AtividadeRAD atividadeRAD, ExtensaoAtividade extensaoAtividade, Importacao importacao, List<String> listCamposIgnorados, Boolean isUpdate) {
        Activity importar;
        long epochDataImportacao = importacaoRAD.getDataImportacao().toInstant(ZoneOffset.UTC).toEpochMilli();
        XMLGregorianCalendar xc = null;
        try {
            xc = DatatypeFactory.newInstance().newXMLGregorianCalendar(importacaoRAD.getDataImportacao().toString());
        } catch (DatatypeConfigurationException e) {
            throw new RuntimeException(e);
        }
        statements.put("importacaoRAD", buildImportacaoRADEntity(importacaoRAD, epochDataImportacao));
        statements.put("atividadeRAD", buildAtividadeEntity(atividadeRAD, epochDataImportacao));
        statements.put("extensaoRAD", buildExtensaoEntity(extensaoAtividade, epochDataImportacao, listCamposIgnorados));

        HadMember hadMember = pFactory.newHadMember(((Entity)statements.get("importacaoRAD")).getId(),Arrays.asList(((Entity)statements.get("atividadeRAD")).getId(),((Entity)statements.get("extensaoRAD")).getId()));

        if (isUpdate) {
            importar = pFactory.newActivity(qName("update/" + epochDataImportacao));
            long previousDataImportacao = provenanceDAO.findFirstByIdAndOpOrderByData_ImportacaoDesc(importacaoRAD.getId(), "UPSERT");
            buildMentionOf((Entity)statements.get("importacaoRAD"),IMP_PREF + importacaoRAD.getId() + "/" + previousDataImportacao,"bundle/"+importacaoRAD.getId()+"/"+previousDataImportacao);
        }else{
            importar = pFactory.newActivity(qName("insert/" + epochDataImportacao));
        }
        importar.setStartTime(xc);
        statements.put("importador", importar);

        WasGeneratedBy wgb = pFactory.newWasGeneratedBy((Entity) statements.get("importacaoRAD"), null, importar);
        statements.put("wgb", wgb);

        if (!CollectionUtils.isEmpty(objetosSecundarios)) {
            int i = 1;
            for (Object obj : objetosSecundarios) {
                statements.put("objSecundario" + i, buildGeneralEntity(obj, epochDataImportacao));
                if (obj instanceof BancaRAD || obj instanceof OrientandoRAD) hadMember.getEntity().add(((Entity) statements.get("objSecundario" + i)).getId());
                else {
                    wgb = pFactory.newWasGeneratedBy((Entity) statements.get("objSecundario" + i), null, importar);
                    statements.put("wgbSec" + i, wgb);
                }
                i++;
            }
        }
        statements.put("hadMember", hadMember);

        Bundle bdl = pFactory.newNamedBundle(qName("bundle/" + importacaoRAD.getId() + "/" + epochDataImportacao), statements.values());
        bdl.setNamespace(Namespace.gatherNamespaces(bdl));
        Document document = pFactory.newDocument();
        document.getStatementOrBundle().add(bdl);
        document.setNamespace(ns);
        return document;
    }

    /**
     * PRODUTO
     */
    public Document buildBundle(Importacao importacao, ImportacaoRAD importacaoRAD, ProdutoRAD produtoRAD, Boolean isUpdate) {
        Activity importar;
        long epochDataImportacao = importacaoRAD.getDataImportacao().toInstant(ZoneOffset.UTC).toEpochMilli();
        XMLGregorianCalendar xc = null;
        try {
            xc = DatatypeFactory.newInstance().newXMLGregorianCalendar(importacaoRAD.getDataImportacao().toString());
        } catch (DatatypeConfigurationException e) {
            throw new RuntimeException(e);
        }

        statements.put("importacaoRAD", buildImportacaoRADEntity(importacaoRAD, epochDataImportacao));
        statements.put("produtoRAD", buildProdutoEntity(produtoRAD, epochDataImportacao));

        HadMember hadMember = pFactory.newHadMember(((Entity)statements.get("importacaoRAD")).getId(),((Entity)statements.get("produtoRAD")).getId());

        if (isUpdate) {
            importar = pFactory.newActivity(qName("update/" + epochDataImportacao));
            long previousDataImportacao = provenanceDAO.findFirstByIdAndOpOrderByData_ImportacaoDesc(importacaoRAD.getId(), "UPSERT");
            buildMentionOf((Entity)statements.get("importacaoRAD"),IMP_PREF + importacaoRAD.getId() + "/" + previousDataImportacao,"bundle/"+importacaoRAD.getId()+"/"+previousDataImportacao);
        }else{
            importar = pFactory.newActivity(qName("insert/" + epochDataImportacao));
        }
        importar.setStartTime(xc);
        statements.put("importador", importar);

        WasGeneratedBy wgb = pFactory.newWasGeneratedBy((Entity) statements.get("importacaoRAD"), null, importar);
        statements.put("wgb", wgb);

        if (!CollectionUtils.isEmpty(objetosSecundarios)) {
            int i = 1;
            for (Object obj : objetosSecundarios) {
                statements.put("objSecundario" + i, buildGeneralEntity(obj, epochDataImportacao));
                if (obj instanceof AutorProdutoRAD || obj instanceof AreaConhecimentoRAD) hadMember.getEntity().add(((Entity) statements.get("objSecundario" + i)).getId());
                else {
                    wgb = pFactory.newWasGeneratedBy((Entity) statements.get("objSecundario" + i), null, importar);
                    statements.put("wgbSec" + i, wgb);
                }
                i++;
            }
        }
        statements.put("hadMember", hadMember);

        Bundle bdl = pFactory.newNamedBundle(qName("bundle/" + importacaoRAD.getId() + "/" + epochDataImportacao), statements.values());
        bdl.setNamespace(Namespace.gatherNamespaces(bdl));
        Document document = pFactory.newDocument();
        document.getStatementOrBundle().add(bdl);
        document.setNamespace(ns);
        return document;
    }

    /**
     * Constrói Bundle para importação do tipo DELETE
     */
    public Document buildBundle(long epochDataImportacao, Importacao importacao, IdsImportacaoAtividadeDerivada iid, long previousDataImportacao){
        Activity importar = pFactory.newActivity(qName("invalidated/" + epochDataImportacao));
        XMLGregorianCalendar xc = null;
        try {
            xc = DatatypeFactory.newInstance().newXMLGregorianCalendar(importacao.getDataImportacao().toString());
        } catch (DatatypeConfigurationException e) {
            throw new RuntimeException(e);
        }
        importar.setStartTime(xc);
        statements.put("invalidation", importar);

        WasInvalidatedBy wib = pFactory.newWasInvalidatedBy(qName("invalidation"), qName(IMP_PREF + iid.getId() + "/" +  previousDataImportacao), importar.getId());
        wib.setTime(xc);
        wib.getLabel().add(pFactory.newInternationalizedString("invalidated"));;
        statements.put("wib", wib);

        Bundle bdl = pFactory.newNamedBundle(qName("bundle/" + iid.getId() + "/" + epochDataImportacao), statements.values());
        bdl.setNamespace(Namespace.gatherNamespaces(bdl));
        Document document = pFactory.newDocument();
        document.getStatementOrBundle().add(bdl);
        document.setNamespace(ns);
        return document;
    }

    /**
     * Constrói a entidade do tipo Bundle que comportará toda a importação
     * e a insere em banco no formato JSON.
     * ATIVIDADE
     * @param isUpdate sinaliza se a importação é uma atualização
     */
    public void makeBundleAndInsert(Importacao importacao, ImportacaoRAD importacaoRAD, AtividadeRAD atividadeRAD, ExtensaoAtividade extensaoRAD, List<String> listCamposIgnorados, Boolean isUpdate) {
        Document document = null;
        document = buildBundle(importacaoRAD, atividadeRAD, extensaoRAD, importacao, listCamposIgnorados, isUpdate);
        String jsonString = documentToJSON(document);
        provenanceDAO.insert(importacaoRAD.getId(), importacaoRAD.getDataImportacao(), jsonString, importacaoRAD.getOperacao().toString());
    }

    /**
     * Constrói a entidade do tipo Bundle que comportará toda a importação
     * e a insere em banco no formato JSON.
     * PRODUTO
     * @param isUpdate sinaliza se a importação é uma atualização
     */
    public void makeBundleAndInsert(Importacao importacao, ImportacaoRAD importacaoRAD, ProdutoRAD produtoRAD, Boolean isUpdate) {
        Document document = null;
        document = buildBundle(importacao, importacaoRAD, produtoRAD, isUpdate);
        String jsonString = documentToJSON(document);
        provenanceDAO.insert(importacaoRAD.getId(), importacaoRAD.getDataImportacao(), jsonString, importacaoRAD.getOperacao().toString());
    }

    /**
     * Método para salvar importações do tipo UPSERT
     * @param isUpdate sinaliza se a importação é uma atualização
     */
    public void save(ImportacaoRAD importacaoRAD, AtividadeRAD atividadeRAD, Importacao importacao, ExtensaoAtividade extensaoRAD, List<String> listCamposIgnorados, Boolean isUpdate) {
        makeBundleAndInsert(importacao,importacaoRAD, atividadeRAD, extensaoRAD, listCamposIgnorados, isUpdate);
    }

    public void save(ImportacaoRAD importacaoRAD, AtividadeRAD atividadeRAD, Importacao importacao, ExtensaoAtividade extensaoRAD, Boolean isUpdate) {
        makeBundleAndInsert(importacao,importacaoRAD, atividadeRAD, extensaoRAD, null, isUpdate);
    }

    public void save (Importacao importacao, ImportacaoRAD importacaoRAD, ProdutoRAD produtoRAD, Boolean isUpdate) {
        makeBundleAndInsert(importacao, importacaoRAD, produtoRAD, isUpdate);
    }

    /**
     * Método para salvar importações do tipo DELETE
     */
    public void save(Importacao importacao,IdsImportacaoAtividadeDerivada iid) {
        long previousDataImportacao = provenanceDAO.findFirstByIdAndOpOrderByData_ImportacaoDesc(iid.getId(), "UPSERT");
        //long epochDataImportacao = importacao.getDataImportacao().toEpochSecond(ZoneOffset.UTC);
        long epochDataImportacao = importacao.getDataImportacao().toInstant(ZoneOffset.UTC).toEpochMilli();
        Document document = buildBundle(epochDataImportacao, importacao, iid, previousDataImportacao);
        String jsonString = documentToJSON(document);
        provenanceDAO.insert(iid.getId(), importacao.getDataImportacao(), jsonString, Importacao.Operacao.DELETE.toString());
    }

}

