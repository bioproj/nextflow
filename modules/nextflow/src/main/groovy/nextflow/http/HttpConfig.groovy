package nextflow.http

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

@ToString(includePackage = false, includeNames = true)
@EqualsAndHashCode(includeFields = true)
@CompileStatic
class HttpConfig {

    private String urlId
    private String urlName


    HttpConfig(Map map){
        def config = map ?: Collections.emptyMap()
        urlId = config.urlId
        urlName = config.urlName
    }

    String getUrlId() {
        return urlId
    }

    String getUrlName() {
        return urlName
    }
}
