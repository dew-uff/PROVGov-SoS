package br.uff.sti.sisiad.consumidor.rad.aspects;

import br.uff.sti.sisiad.consumidor.rad.modelo.IdiomaRAD;
import br.uff.sti.sisiad.consumidor.rad.provenance.ProvenanceExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect IdiomaAspect percflow(annotatedMethod()) {

    private IdiomaRAD novoIdioma = null;
    private Logger log = LoggerFactory.getLogger(IdiomaAspect.class);

    pointcut executeSaveMethod(): execution(* br.uff.sti.sisiad.consumidor.rad.dao.IdiomaRADDAO.insert(..));
    pointcut annotatedMethod(): execution(@br.uff.sti.sisiad.consumidor.rad.annotations.TraceIdiomaCreationMethod * *(..));

    /**
     * Aspecto disparado durante a execução dos métodos anotados com @TraceIdiomaCreationMethod
     * O aspecto depende do término da execução de IdiomaRADDAO.insert para verificar se
     * um novo idioma foi salvo em banco.
     * Aspectos são singletons por padrão, em razão disso o modelo de instanciação percflow foi utilizado.
     * https://eclipse.dev/aspectj/doc/released/progguide/semantics-aspects.html#aspect-instantiation
     */
    Object around(): annotatedMethod(){
        Object retVal = proceed();
        log.info("Aspect from annotated method executing: "+ thisJoinPoint.getSignature().toShortString());
        if (novoIdioma != null) ((ProvenanceExtension) thisJoinPoint.getThis()).getProvenanceRADService().getObjetosSecundarios().add(novoIdioma);
        return retVal;
    }

    /**
     * Aspecto disparado após a chamada ao método IdiomaRADDAO.insert
     * (dentro do fluxo de controle do método ProdutoRADService.criaProduto).
     * Altera a variável de controle para sinalizar que um novo idioma
     * foi salvo em banco.
     */
    after() returning(IdiomaRAD idiomaRAD): executeSaveMethod() && cflowbelow(annotatedMethod()){
        novoIdioma = idiomaRAD;
        log.info("Aspect triggered for: "+ thisJoinPoint.getSignature().toShortString());
    }

}
