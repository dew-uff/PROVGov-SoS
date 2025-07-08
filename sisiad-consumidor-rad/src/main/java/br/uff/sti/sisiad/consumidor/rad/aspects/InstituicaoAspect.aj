package br.uff.sti.sisiad.consumidor.rad.aspects;

import br.uff.sti.sisiad.consumidor.rad.modelo.InstituicaoRAD;
import br.uff.sti.sisiad.consumidor.rad.provenance.ProvenanceExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect InstituicaoAspect percflow(annotatedMethod()) {

    private InstituicaoRAD novaInstituicao = null;
    private Logger log = LoggerFactory.getLogger(InstituicaoAspect.class);

    pointcut executeSaveMethod(): execution(* br.uff.sti.sisiad.consumidor.rad.dao.InstituicaoRADDAO.save(..));
    pointcut annotatedMethod(): execution(@br.uff.sti.sisiad.consumidor.rad.annotations.TraceInstituicaoCreationMethod * *(..));

    /**
     * Aspecto disparado durante a execução dos métodos anotados com @TraceInstituicaoCreationMethod
     * O aspecto depende do término da execução de InstituicaoRADDAO.save para verificar se
     * uma nova instituição foi salva em banco.
     * Aspectos são singletons por padrão, em razão disso o modelo de instanciação percflow foi utilizado.
     * https://eclipse.dev/aspectj/doc/released/progguide/semantics-aspects.html#aspect-instantiation
     */
    Object around(): annotatedMethod(){
        Object retVal = proceed();
        log.info("Aspect from annotated method executing: "+ thisJoinPoint.getSignature().toShortString());
        if (novaInstituicao != null) ((ProvenanceExtension) thisJoinPoint.getThis()).getProvenanceRADService().getObjetosSecundarios().add(novaInstituicao);
        return retVal;
    }

    /**
     * Aspecto disparado após a chamada ao método InstituicaoRADDAO.save
     * (dentro do fluxo de controle do método BancaRADService.criaEspecialBanca).
     * Altera a variável de controle para sinalizar que uma nova instituição
     * foi salva em banco.
     */
    after() returning(InstituicaoRAD instituicaoRAD): executeSaveMethod() && cflowbelow(annotatedMethod()){
        novaInstituicao = instituicaoRAD;
        log.info("Aspect triggered for: "+ thisJoinPoint.getSignature().toShortString());
    }

}
