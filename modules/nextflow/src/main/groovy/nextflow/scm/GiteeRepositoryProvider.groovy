package nextflow.scm

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic

@CompileStatic
class GiteeRepositoryProvider extends RepositoryProvider{
    GiteeRepositoryProvider(String project, ProviderConfig config=null) {
        this.project = project
        this.config = config ?: new ProviderConfig('gitee')
    }

    /** {@inheritDoc} */
    @Override
    String getName() { "Gitee" }

    /** {@inheritDoc} */
    @Override
    String getEndpointUrl() {
        "${config.server}/${project}"
    }

    @Override
    protected void auth( URLConnection connection ) {
        if( config.token ) {
            // set the token in the request header
            // https://docs.gitea.io/en-us/api-usage/#authentication
            connection.setRequestProperty("Authorization", "token $config.token")
        }
        else {
            super.auth(connection)
        }
    }

    @Override
    @CompileDynamic
    List<BranchInfo> getBranches() {
        // https://try.gitea.io/api/swagger#/repository/repoListBranches
        final url = "${config.endpoint}/repos/${project}/branches"
        this.<BranchInfo>invokeAndResponseWithPaging(url, { Map branch -> new BranchInfo(branch.name as String, branch.commit?.id as String) })
    }

    @Override
    @CompileDynamic
    List<TagInfo> getTags() {
        // https://try.gitea.io/api/swagger#/repository/repoListTags
        final url = "${config.endpoint}/repos/${project}/tags"
        this.<TagInfo>invokeAndResponseWithPaging(url, { Map tag -> new TagInfo(tag.name as String, tag.commit?.sha as String) })
    }

    /** {@inheritDoc} */
    @Override
    String getContentUrl( String path ) {
        // see
        // https://try.gitea.io/api/swagger#/repository/repoGetRawFile
        // note: `ref` is undocumented
        def result = "${config.endpoint}/$project/contents/$path"
        if( revision )
            result += "?ref=$revision"
        return result
    }

    /** {@inheritDoc} */
    @Override
    String getCloneUrl() {
//        Map response = invokeAndParseResponse( getEndpointUrl() )
//
//        def result = response.get('clone_url')
//        if( !result )
//            throw new IllegalStateException("Missing clone URL for: $project")

        return getEndpointUrl()
    }

    /** {@inheritDoc} */
    @Override
    String getRepositoryUrl() {
        "${config.server}/$project"
    }

    /** {@inheritDoc} */
    @Override
    byte[] readBytes(String path) {
//        def url = getContentUrl(path)
//
//        def contents = invoke(url)
//        return contents?.getBytes()
        def url = getContentUrl(path)
        Map response  = invokeAndParseResponse(url)
        response.get('content')?.toString()?.decodeBase64()
    }
}
