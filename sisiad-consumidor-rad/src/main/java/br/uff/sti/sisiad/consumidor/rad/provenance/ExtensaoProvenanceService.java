package br.uff.sti.sisiad.consumidor.rad.provenance;

import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.*;
import org.springframework.data.annotation.Id;
import org.springframework.util.CollectionUtils;

import java.io.ByteArrayOutputStream;
import java.io.Serial;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public abstract class ExtensaoProvenanceService {

    //protected final Namespace ns;
    public final ProvFactory pFactory;
    public final String customPrefix;
    public final String customNamespace;
    protected final Namespace ns;

    protected final Name name;

    protected ExtensaoProvenanceService(String prefix,String namespace) {
        this.customPrefix = prefix;
        this.customNamespace = namespace;
        this.pFactory = InteropFramework.getDefaultFactory();
        this.name=this.pFactory.getName();
        this.ns=new Namespace();
        this.ns.addKnownNamespaces();
        this.ns.register(this.customPrefix, customNamespace);
    }

    public QualifiedName qName(String n) {
        return pFactory.newQualifiedName(customNamespace, n, customPrefix);
        //return ns.qualifiedName(myPrefix, n, pFactory);
    }

    /*public QualifiedName primQName(String n) {
        return pFactory.newQualifiedName(PRIM_NS, n, PRIM_PREFIX);
    }*/

    public Entity newEntityExtended (String ettName, String ettType){
        Entity et = pFactory.newEntity(qName(ettName));
       if ((ettType != null) && ettType.length() != 0){
            pFactory.addType(et,pFactory.newType(ettType,name.PROV_TYPE));
        }
        return et;
    }

    public Entity newEntityExtended (String ettName, Object ettValue){
        Entity et = pFactory.newEntity(qName(ettName));
         if (ettValue != null){
            et.setValue(pFactory.newValue(ettValue,name.PROV_VALUE));
        }
        return et;
    }
    public Entity newEntityExtended (String ettName, String ettLocation, String ettType, Object ettValue){
        Entity et = pFactory.newEntity(qName(ettName));
        if ((ettLocation != null) && ettLocation.length() != 0){
            et.getLocation().add(pFactory.newLocation(ettLocation,name.PROV_LOCATION));
        }
        if ((ettType != null) && ettType.length() != 0){
            pFactory.addType(et,pFactory.newType(ettType,name.PROV_TYPE));
        }
        if (ettValue != null){
            et.setValue(pFactory.newValue(ettValue,name.PROV_VALUE));
        }
      return et;
    }

    public Entity newEntityExtended (String ettName, String ettLocation, String ettType, Object ettValueHash, Object ettValue){
        Entity et = pFactory.newEntity(qName(ettName));
        if ((ettLocation != null) && ettLocation.length() != 0){
            et.getLocation().add(pFactory.newLocation(ettLocation,name.PROV_LOCATION));
        }
        if ((ettType != null) && ettType.length() != 0){
            pFactory.addType(et,pFactory.newType(ettType,name.PROV_TYPE));
        }
        if (ettValueHash != null){
            et.setValue(pFactory.newValue(ettValueHash,name.PROV_VALUE));
        }
        if (ettValue != null){
            et.setValue(pFactory.newValue(ettValue,name.PROV_VALUE));
        }
        return et;
    }

    public Entity newEntityExtended (String ettName, String ettLocation, String ettType, List <Attribute> ettAtributes){
        Entity et = pFactory.newEntity(qName(ettName));
        if ((ettLocation != null) && ettLocation.length() != 0){
            et.getLocation().add(pFactory.newLocation(ettLocation,name.PROV_LOCATION));
        }
        if ((ettType != null) && ettType.length() != 0){
            pFactory.addType(et,pFactory.newType(ettType,name.PROV_TYPE));
        }
        if (!CollectionUtils.isEmpty(ettAtributes)) {
            pFactory.setAttributes(et,ettAtributes);
        }
        return et;
    }

    public Entity newEntityExtended (String ettName, String ettLocation, String ettType, Object ettValue, List <Attribute> ettAtributes){
        Entity et = pFactory.newEntity(qName(ettName));
        /*if ((ettLabel != null) && ettLabel.length() != 0){
            pFactory.addLabel(et, ettLabel);
        }*/
        if ((ettLocation != null) && ettLocation.length() != 0){
            et.getLocation().add(pFactory.newLocation(ettLocation,name.PROV_LOCATION));
        }
        if ((ettType != null) && ettType.length() != 0){
            pFactory.addType(et,pFactory.newType(ettType,name.PROV_TYPE));
        }
        if (ettValue != null){
            et.setValue(pFactory.newValue(ettValue,name.PROV_VALUE));
        }
        if (!CollectionUtils.isEmpty(ettAtributes)) {
            pFactory.setAttributes(et,ettAtributes);
        }
        return et;
    }

    public String convertTime(LocalDateTime dateTime) {
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(dateTime);
    }

    /*
    Current MimeTypeMap (key=value). Use the value for media type:
    JSONLD=application/ld+json, PNG=image/png, TURTLE=text/turtle, JPEG=image/jpeg,
    JSON=application/json, TRIG=application/trig, SVG=image/svg+xml,
    PROVN=text/provenance-notation, PROVX=application/provenance+xml, PDF=application/pdf,
    DOT=text/vnd.graphviz
    */
    public String documentToJSON(Document document) {
        InteropFramework intF=new InteropFramework();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        //intF.writeDocument(stream, document, Formats.ProvFormat.JSON);
        intF.writeDocument(stream,document,"application/json",false);
        //intF.writeDocument("testeVisualizacao.png", document);
        String result = stream.toString(StandardCharsets.UTF_8);
        //System.out.println(result);
        return result;
        //return result.replace("\n", " ");
    }

    public String documentToPROVN(Document document) {
        InteropFramework intF=new InteropFramework();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        //ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
        //intF.writeDocument(stream, document, Formats.ProvFormat.PROVN);
        intF.writeDocument(stream,document,"text/provenance-notation",false);
        //intF.writeDocument("testeVisualizacao.png", document);

        //intF.writeDocument(stream2,document,"application/json",true);
        String result = stream.toString(StandardCharsets.UTF_8);
        //String result2 = stream2.toString(StandardCharsets.UTF_8);
        //System.out.println(result);
        //System.out.println("****************************************");
        //System.out.println(result2);
        return result;
        // return resultJSON.replace("\n", "");
    }

    //<field name (String), hashCode (int)>
    public Map<String, Integer> getFieldsHashCodeExceptPublicAndId(Object object){
        Map<String,Integer> fieldsHashCodes = new HashMap<>();
        Object value = null;
        for (Field field : object.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            if ((field.getAnnotation(Id.class) == null) && !(Modifier.isPublic(field.getModifiers()))){
                try {
                    value = field.get(object);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                fieldsHashCodes.put(field.getName(),Objects.hashCode(value));
            }
        }
        return fieldsHashCodes;
    }

    public Map<String, String> getFieldsExceptPublicAndId(Object object){
        Map<String,String> fields = new HashMap<>();
        Object value = null;
        for (Field field : object.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            if ((field.getAnnotation(Id.class) == null) && !(Modifier.isPublic(field.getModifiers()))){
                try {
                    value = field.get(object);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                fields.put(field.getName(),Objects.toString(value));
            }
        }
        return fields;
    }

    public Map<String, String> getFieldsExceptPublic(Object object){
        Map<String,String> fields = new HashMap<>();
        Object value = null;
        for (Field field : object.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            if (!(Modifier.isPublic(field.getModifiers()))){
                try {
                    value = field.get(object);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                fields.put(field.getName(),Objects.toString(value));
            }
        }
        return fields;
    }
    public Map<String, String> getFieldsExceptPublic2(Object object){
        Map<String,String> fields = new HashMap<>();
        Object value = null;
        for (Field field : object.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            if ((Modifier.isPrivate(field.getModifiers()))){
                try {
                    value = field.get(object);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                fields.put(field.getName(),Objects.toString(value));
            }
        }
        return fields;
    }


    public Map convertAllFieldsHashCodeStringToMap(String mapString){
        Map<String, Integer> map = new HashMap<>();
        StringTokenizer tokenizer = new StringTokenizer(mapString.replaceAll("[{}]",""), ",");
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            String[] keyValue = token.split("=");
            map.put(keyValue[0].replaceAll("^\\s",""), Integer.parseInt(keyValue[1]));
        }
        return map;
    }

}

