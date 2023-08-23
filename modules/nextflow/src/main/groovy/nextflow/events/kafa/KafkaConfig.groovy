package nextflow.events.kafa

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * @author : jorge <jorge.aguilera@seqera.io>
 *
 */
@ToString(includePackage = false, includeNames = true)
@EqualsAndHashCode(includeFields = true)
@CompileStatic
class KafkaConfig {

    private String url
    private String group

    KafkaConfig(Map map){
        def config = map ?: Collections.emptyMap()
        url = config.url
        group = config.group
    }

    String getUrl(){
        url
    }

    String getGroup(){
        group
    }

}
