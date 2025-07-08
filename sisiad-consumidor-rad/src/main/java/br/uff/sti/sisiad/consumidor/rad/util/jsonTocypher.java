package br.uff.sti.sisiad.consumidor.rad.util;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.StringTokenizer;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.lang3.StringUtils;
import org.example.JDBCUtils;
import org.openprovenance.prov.interop.Formats;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Activity;
import org.openprovenance.prov.model.Bundle;
import org.openprovenance.prov.model.Document;
import org.openprovenance.prov.model.Statement;
import org.openprovenance.prov.model.Value;
import org.openprovenance.prov.model.WasGeneratedBy;
import org.openprovenance.prov.model.WasInvalidatedBy;
import org.openprovenance.prov.model.Type;
import org.openprovenance.prov.model.Entity;
import org.openprovenance.prov.model.HadMember;
import org.openprovenance.prov.model.MentionOf;
import org.openprovenance.prov.model.QualifiedName;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

import org.example.convert.KeyValueParser;

public class jsonTocypher {

    static final String dbUri = "";
    static final String dbUser = "";
    static final String dbPassword = "";
    static HashMap<QualifiedName,String> nodes = new HashMap<>();
    static final String QUERY = "";

    public static Session createSession(){
        var driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword));
        var session = driver.session(SessionConfig.builder().withDatabase("neo4j").build());
        return session;
    }

    public static void createNodes(HashMap<String,Entity> entities, Transaction tx){
        String cypher = "";

        for (Map.Entry<String, Entity> entry : entities.entrySet()) {
            String key = entry.getKey();
            Entity entity = entry.getValue();
            Value val = ((Entity) entity).getValue();
            List<Type> type = ((Entity) entity).getType();

            if (val !=null){
                String value = val.getValue().toString();
                value = value.replaceAll("^.|.$", "");
                value = value.replaceAll("\"", "\'");
                Map<String,String> mapa = KeyValueParser.parse(value);
                cypher = "CREATE (node:Entity {name:\"" + key + "\",";
                for (Map.Entry<String, String> entryVal : mapa.entrySet()) {
                    cypher = cypher + entryVal.getKey() + ":\"" + entryVal.getValue()+"\",";
                }

                cypher = StringUtils.chop(cypher);
                cypher = cypher + "}) RETURN ID(node) as ID";
            } else{
                if (!type.isEmpty()){
                    cypher = "CREATE (node:Entity {name:\"" + key + "\",type:\"" + (type.get(0)).getValue() +"\"}) RETURN ID(node) as ID";
                }
            }      
            var result = tx.run(cypher).single();
            var id = result.get("ID");
            nodes.put(entity.getId(),id.toString());
        }
    }

    public static void createCollection(Queue<QualifiedName> entities, Transaction tx){
        String cypher = "";

        if(!entities.isEmpty()){
            QualifiedName coll = entities.remove();
            var collId = nodes.get(coll);
        
            while (entities.peek() != null) {
                var entID = nodes.get(entities.remove());
                cypher = "MATCH (c:Entity),(e:Entity) WHERE ID(c) = "+ collId +" AND ID(e) = "+ entID +" CREATE (c)-[:HAD_MEMBER]->(e)";
                tx.run(cypher);
            }
        }
    }

    public static void createMention(Queue<MentionOf> men, Transaction tx){
        while (men.peek() != null) {
            MentionOf mention = men.remove();
            var specifId = nodes.get(mention.getSpecificEntity());
            var result = tx.run("MATCH (n:Entity) WHERE n.name=\"" + mention.getGeneralEntity().getLocalPart() + "\" RETURN ID(n)").single();
            var generalId = result.get("ID(n)");
            String cypher = "MATCH (spe:Entity),(gen:Entity) WHERE ID(spe) = "+ specifId.toString() +" AND ID(gen) = "+ generalId.toString() +" CREATE (spe)-[:MENTION]->(gen)";
            tx.run(cypher);
        }
    }

    public static void createRelation(Queue<Statement> act, Transaction tx){
        while (act.peek() != null) {
            WasInvalidatedBy rel = (WasInvalidatedBy)act.remove();
            XMLGregorianCalendar actvtTime = rel.getTime();
            String label = rel.getLabel().get(0).getValue();
            String enttName = rel.getEntity().getLocalPart();
            var result = tx.run("MATCH (n:Entity) WHERE n.name=\"" + enttName + "\" RETURN ID(n)").single();
            var enttId = result.get("ID(n)");
            String cypher = "MATCH (e:Entity) WHERE ID(e) = "+ enttId +" CREATE (e)-[:" + label + "{ time: \"" + actvtTime + "\" }]->(e)";
            tx.run(cypher);
        }

    }

    public static QualifiedName relationHelperGetActivity(Statement w){
        InteropFramework intF=new InteropFramework();

        QualifiedName temp = null;  
        if (w instanceof WasGeneratedBy){
            temp = ((WasGeneratedBy)w).getActivity();
        } else if (w instanceof WasInvalidatedBy){
            temp = ((WasInvalidatedBy)w).getActivity();
        }
        return temp;
    }

    public static String relationHelperGetEntity(Statement w){
        String temp = "";  
        if (w instanceof WasGeneratedBy){
            temp = ((WasGeneratedBy)w).getEntity().getLocalPart();
        } else if (w instanceof WasInvalidatedBy){
            temp = ((WasInvalidatedBy)w).getEntity().getLocalPart();
        }
        return temp;
    }

    public static void main(String [] args) throws IOException {

            String json;
            Document document = null;
            InteropFramework intF=new InteropFramework();

            HashMap<String,Entity> entities = new HashMap<>();
            Queue<QualifiedName> qColl = new LinkedList<>();
            Queue<MentionOf> qMen = new LinkedList<>();
            Queue<Statement> qRel = new LinkedList<>();

            try (Connection connection = JDBCUtils.getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(QUERY);) {
                ResultSet rs = preparedStatement.executeQuery();
                
                Session session = createSession();

                while (rs.next()) {
                    Transaction tx = session.beginTransaction();
                    json = rs.getString("prov_json");
                    InputStream targetStream = new ByteArrayInputStream(json.getBytes());
                    document = intF.readDocument(targetStream, Formats.ProvFormat.JSON);
                    Bundle b = (Bundle) document.getStatementOrBundle().get(0);
                    List<Statement> s = b.getStatement();
                    Iterator<Statement> dataIterator = s.iterator();

                    boolean isUpdate = false;
                    boolean isDelete = false;
                    String node = "";
                    entities.clear();
                    qColl.clear();
                    qMen.clear();
                    qRel.clear();
                    while(dataIterator.hasNext()) {
                        Statement data = dataIterator.next();
                        switch(data.getKind()) {
                            case PROV_ENTITY:
                                entities.put(((Entity) data).getId().getLocalPart(), (Entity) data);
                                break;
                            case PROV_ACTIVITY:
                                String qName = ((Activity) data).getId().getLocalPart();
                                break;
                            case PROV_USAGE:
                                break;
                            case PROV_MENTION:
                                qMen.add(((MentionOf) data));
                                break;
                            case PROV_MEMBERSHIP:
                                qColl.add(((HadMember) data).getCollection());
                                qColl.addAll(((HadMember) data).getEntity());
                                break;
                            case PROV_GENERATION:
                                break;
                            case PROV_INVALIDATION:
                                qRel.add(data);
                                break;
                            default:
                        } 
                    }
                    createNodes(entities, tx);
                    createCollection(qColl, tx);
                    createMention(qMen, tx);
                    createRelation(qRel, tx);
                    tx.commit();
                    tx.close();
                }
                session.close();
            } catch (SQLException e) {
                JDBCUtils.printSQLException(e);
            }
    }
}
