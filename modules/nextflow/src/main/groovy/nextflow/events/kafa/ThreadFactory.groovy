package nextflow.events.kafa

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.util.ThreadPoolBuilder

import java.util.concurrent.ExecutorService


/**
 * A factory of threads who maintains references to finish them properly
 *
 * @author : jorge <jorge.aguilera@seqera.io>
 *
 */
@Slf4j
@Singleton
@CompileStatic
class ThreadFactory {

    private List<ExecutorService> executors = []

    ExecutorService createExecutor(String topic){
        def executor = ThreadPoolBuilder.io(
                1,
                1,
                1,
                "kafka-$topic")
        log.trace("Created executor service for topic $topic")
        executors.add executor
        executor
    }

    void shutdownExecutors() {
        log.info "Closing kafka {} listeners", executors.size()
        executors.each{
            log.trace "Closing $it"
            it.shutdownNow()
        }
        log.info("Kafka topics awaiting completion");
        executors.clear()
    }


}
