/*
 * Copyright 2020-2022, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package nextflow.events


import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.NF
import nextflow.Session
import nextflow.events.kafa.PublisherTopic
import nextflow.events.kafa.TopicHandler
import nextflow.extension.CH
import nextflow.events.kafa.KafkaConfig
import nextflow.extension.DataflowHelper
import nextflow.plugin.extension.Factory
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.Operator
import nextflow.plugin.extension.PluginExtensionPoint

import java.time.Duration

/**
 * Provide a channel factory extension that allows the execution of Sql queries
 *
 * @author Jorge Aguilera <jorge.aguilera@seqera.io>
 */
@Slf4j
@CompileStatic
class ChannelKafkaExtension extends PluginExtensionPoint {

    private Session session
    private KafkaConfig config

    protected void init(Session session) {
        this.session = session
        this.config = new KafkaConfig( session.config.navigate('kafka') as Map)
    }

    @Factory
    DataflowWriteChannel fromTopic(String topic, Duration duration=Duration.ofSeconds(1)) {
        topicToChannel(topic, duration, false)
    }

    @Factory
    DataflowWriteChannel watchTopic(String topic, Duration duration=Duration.ofSeconds(1)) {
        topicToChannel(topic, duration, true)
    }

    @Operator
    DataflowWriteChannel publishTopic(DataflowReadChannel source, String topic){
        final target = CH.createBy(source)
        final next = {
            new PublisherTopic()
                    .withUrl(config.url)
                    .withGroup(config.group)
                    .withTopic(topic)
                    .publishMessage(it)
            target.bind(it)
        }
        final done = {
            target.bind(Channel.STOP)
        }
        DataflowHelper.subscribeImpl(source, [onNext: next, onComplete: done])
        target
    }

    @Function
    void writeMessage(String topic, String message, String key=null){
        new PublisherTopic()
                .withUrl(config.url)
                .withGroup(config.group)
                .withTopic(topic)
                .publishMessage([key, message])
    }

    private DataflowWriteChannel topicToChannel(String topic, Duration duration, boolean listening){
        final channel = CH.create()

        final handler = new TopicHandler()
                .withSession(this.session)
                .withUrl(config.url)
                .withGroup(config.group)
                .withTopic(topic)
                .withListening(listening)
                .withTarget(channel)
                .withDuration(duration)
        if(NF.dsl2) {
            session.addIgniter {-> handler.perform() }
        }
        else {
            handler.perform()
        }
        return channel
    }

}
