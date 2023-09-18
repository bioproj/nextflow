package nextflow.http

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

@ToString(includePackage = false, includeNames = true)
@EqualsAndHashCode(includeFields = true)
@CompileStatic
class HttpConfig {

    private String url
    private String group

    HttpConfig(Map map){
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
