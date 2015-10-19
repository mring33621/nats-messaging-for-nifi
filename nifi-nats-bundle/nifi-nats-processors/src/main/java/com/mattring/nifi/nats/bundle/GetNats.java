/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mattring.nifi.nats.bundle;

import static com.mattring.nifi.nats.bundle.AbstractNatsProcessor.CHARSET;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.nats.Connection;
import org.nats.MsgHandler;

@SupportsBatching
@CapabilityDescription("Fetches messages from a NATS Messaging Topic")
@Tags({"NATS", "Messaging", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Receive"})
@WritesAttributes({
    @WritesAttribute(attribute = "nats.topic", description = "The name of the NATS Topic from which the message was received"),
    @WritesAttribute(attribute = "nats.numMsgs", description = "The number or messages contained in this flowfile")
})
public class GetNats extends AbstractNatsProcessor {

    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("Topic Name")
            .description("The NATS Topic of interest")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("Specifies the maximum number of messages to combine into a single FlowFile. These messages will be "
                    + "concatenated together with the <Message Demarcator> string placed between the content of each message. "
                    + "If the messages from NATS should not be concatenated together, leave this value at 1.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("1")
            .build();
    public static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("Message Demarcator")
            .description("Specifies the characters to use in order to demarcate multiple messages from NATS. If the <Batch Size> "
                    + "property is set to 1, this value is ignored. Otherwise, for each two subsequent messages in the batch, "
                    + "this value will be placed in between them.")
            .required(true)
            .addValidator(Validator.VALID) // accept anything as a demarcator, including empty string
            .expressionLanguageSupported(false)
            .defaultValue("\\n")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are created are routed to this relationship")
            .build();

    private Connection connection;
    private Integer subscription;
    private final Object natsLock;
    private final LinkedBlockingQueue<String> inbox;
    private final MsgHandler msgHandler;

    public GetNats() {
        this.natsLock = new Object();
        // TODO: queue capacity should be a property
        this.inbox = new LinkedBlockingQueue<>();
        this.msgHandler = new MsgHandler() {
            @Override
            public void execute(String msg, String reply, String subject) {
                final boolean enqueued = inbox.offer(msg);
                // TODO: if ! enqueued then do what?
            }
        };
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> props = super.getSupportedPropertyDescriptors();
        props.add(TOPIC);
        props.add(BATCH_SIZE);
        props.add(MESSAGE_DEMARCATOR);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @OnScheduled
    public void subscribe(final ProcessContext context) {
        synchronized (natsLock) {
            if (connection == null) {
                connection = createConnection(context);
            }
            final String topic = context.getProperty(TOPIC).getValue();
            try {
                subscription = connection.subscribe(topic, msgHandler);
            } catch (IOException ex) {
                subscription = null;
                throw new ProcessException("Failed to subscribe to NATS topic: " + topic, ex);
            }
        }
    }

    @OnUnscheduled
    public void unsubscribe(final ProcessContext context) {
        synchronized (natsLock) {
            if (connection != null && subscription != null) {
                final String topic = context.getProperty(TOPIC).getValue();
                try {
                    connection.unsubscribe(subscription);
                } catch (IOException ex) {
                    throw new ProcessException("Failed to unsubscribe from NATS topic: " + topic, ex);
                }
            }
        }
    }

    @OnStopped
    public void shutdownConnection() {
        synchronized (natsLock) {
            if (connection != null) {
                closeWithLogging(connection);
                connection = null;
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final long start = System.nanoTime();

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final String topic = context.getProperty(TOPIC).getValue();

        String flowFileContent = null;
        int numMsgs = 0;
        if (batchSize < 2) {
            flowFileContent = inbox.poll();
            if (flowFileContent != null) {
                numMsgs = 1;
            }
        } else {
            final String demarcator = context.getProperty(MESSAGE_DEMARCATOR).getValue().replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
            final List<String> batch = new LinkedList<>();
            numMsgs = inbox.drainTo(batch, batchSize);
            if ( ! batch.isEmpty() ) {
                flowFileContent = StringUtils.join(batch, demarcator);
            }
        }

        if (flowFileContent != null) {

            FlowFile flowFile = session.create();
            final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
            final byte[] flowFileContentBytes = flowFileContent.getBytes(charset);

            flowFile = session.append(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(flowFileContentBytes);
                }
            });

            final Map<String, String> attributes = new HashMap<>();
            attributes.put("nats.topic", topic);
            attributes.put("nats.numMsgs", Integer.toString(numMsgs));
            flowFile = session.putAllAttributes(flowFile, attributes);
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            session.getProvenanceReporter().receive(flowFile, "nats://" + topic, "Received " + numMsgs + " NATS messages", millis);
            getLogger().info("Successfully received {} from NATS with {} messages in {} millis", new Object[]{flowFile, numMsgs, millis});
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

}
