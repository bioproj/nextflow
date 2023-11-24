package nextflow.http

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

@ToString(includePackage = false, includeNames = true)
@EqualsAndHashCode(includeFields = true)
@CompileStatic
class HttpConfig {

    private String sampleId
    private String urlName
    private String postUrl


    HttpConfig(Map map){
        def config = map ?: Collections.emptyMap()
        sampleId = config.sampleId
        urlName = config.urlName
        postUrl = config.postUrl
    }

    String getSampleId() {
        return sampleId
    }

    void setSampleId(String sampleId) {
        this.sampleId = sampleId
    }

    String getUrlName() {
        return urlName
    }

    void setUrlName(String urlName) {
        this.urlName = urlName
    }

    String getPostUrl() {
        return postUrl
    }

    void setPostUrl(String postUrl) {
        this.postUrl = postUrl
    }
}
