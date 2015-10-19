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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.util.NonThreadSafeCircularBuffer;
import org.apache.nifi.util.LongHolder;
import org.nats.Connection;

@SupportsBatching
@Tags({ "NATS", "Messaging", "Put", "Send", "Message", "PubSub" })
@CapabilityDescription("Sends the contents of a FlowFile as a message to NATS Messaging")
public class PutNats extends AbstractNatsProcessor {

    public static final AllowableValue DELIVERY_BEST_EFFORT = new AllowableValue("0", "Best Effort", "FlowFile will be routed to success after"
        + " successfully writing the content to a NATS node, without waiting for a response. This provides the best performance but may result"
        + " in data loss.");
    // TODO: add DELIVERY_SERVER_ACK AllowableValue; will likely need a new SERVER_ACK_TIMEOUT propertty

    /**
     * AllowableValue for a Producer Type that synchronously sends messages to NATS
     */
    public static final AllowableValue PRODUCER_TYPE_SYNCHRONOUS = new AllowableValue("sync", "Synchronous", "Send FlowFiles to NATS immediately.");
    // TODO: any reason to consider async?

    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
        .name("Topic Name")
        .description("The NATS Topic of interest")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor MESSAGE_DELIMITER = new PropertyDescriptor.Builder()
        .name("Message Delimiter")
        .description("Specifies the delimiter to use for splitting apart multiple messages within a single FlowFile. "
            + "If not specified, the entire content of the FlowFile will be used as a single message. "
            + "If specified, the contents of the FlowFile will be split on this delimiter and each section "
            + "sent as a separate NATS message.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
        .name("Max Buffer Size")
        .description("The maximum amount of data to buffer in memory before sending to NATS")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .expressionLanguageSupported(false)
        .defaultValue("1 MB")
        .build();
// TODO: future
//    public static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
//        .name("Delivery Guarantee")
//        .description("Specifies the requirement for guaranteeing that a message is sent to NATS")
//        .required(true)
//        .expressionLanguageSupported(false)
//        .allowableValues(DELIVERY_BEST_EFFORT)
//        .defaultValue(DELIVERY_BEST_EFFORT.getValue())
//        .build();
//    public static final PropertyDescriptor PRODUCER_TYPE = new PropertyDescriptor.Builder()
//        .name("Producer Type")
//        .description("This parameter specifies whether the messages are sent asynchronously in a background thread.")
//        .required(true)
//        .allowableValues(PRODUCER_TYPE_SYNCHRONOUS)
//        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//        .expressionLanguageSupported(false)
//        .defaultValue(PRODUCER_TYPE_SYNCHRONOUS.getValue())
//        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that is successfully sent to NATS will be routed to this Relationship")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that cannot be sent to NATS will be routed to this Relationship")
        .build();

    final BlockingQueue<Connection> connections;
    
    public PutNats() {
        connections = new LinkedBlockingQueue<>();
    }
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> props = super.getSupportedPropertyDescriptors();
        props.add(TOPIC);
        props.add(MESSAGE_DELIMITER);
        props.add(MAX_BUFFER_SIZE);
// TODO: future
//        props.add(DELIVERY_GUARANTEE);
//        props.add(PRODUCER_TYPE);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(2);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @OnStopped
    public void closeConnections() {
        Connection connection;

        while ((connection = connections.poll()) != null) {
            closeWithLogging(connection);
        }
    }

    Connection borrowConnection(final ProcessContext context) {
        final Connection connection = connections.poll();
        return connection == null ? createConnection(context) : connection;
    }

    void returnConnection(final Connection connection) {
        connections.offer(connection);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long start = System.nanoTime();
        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
        String delimiter = context.getProperty(MESSAGE_DELIMITER).evaluateAttributeExpressions(flowFile).getValue();
        if (delimiter != null) {
            delimiter = delimiter.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        }

        final long maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).longValue();
        final Connection connection = borrowConnection(context);

        if (delimiter == null) {
            // Send the entire FlowFile as a single message.
            final byte[] value = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, value);
                }
            });

            boolean error = false;
            try {
                String message = new String(value, charset);

                connection.publish(topic, message);
                final long nanos = System.nanoTime() - start;

                session.getProvenanceReporter().send(flowFile, "nats://" + topic);
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().info("Successfully sent {} to NATS in {} millis", new Object[] { flowFile, TimeUnit.NANOSECONDS.toMillis(nanos) });
            } catch (final Exception e) {
                getLogger().error("Failed to send {} to NATS due to {}; routing to failure", new Object[] { flowFile, e });
                session.transfer(flowFile, REL_FAILURE);
                error = true;
            } finally {
                if (error) {
                    closeWithLogging(connection);
                } else {
                    returnConnection(connection);
                }
            }
        } else {
            final byte[] delimiterBytes = delimiter.getBytes(charset);

            // The NonThreadSafeCircularBuffer allows us to add a byte from the stream one at a time and see
            // if it matches some pattern. We can use this to search for the delimiter as we read through
            // the stream of bytes in the FlowFile
            final NonThreadSafeCircularBuffer buffer = new NonThreadSafeCircularBuffer(delimiterBytes);

            boolean error = false;
            final LongHolder lastMessageOffset = new LongHolder(0L);
            final LongHolder messagesSent = new LongHolder(0L);

            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream rawIn) throws IOException {
                        byte[] data = null; // contents of a single message

                        boolean streamFinished = false;

                        final List<String> messages = new ArrayList<>(); // batch to send
                        long messageBytes = 0L; // size of messages in the 'messages' list

                        int nextByte;
                        try (final InputStream bufferedIn = new BufferedInputStream(rawIn);
                            final ByteCountingInputStream in = new ByteCountingInputStream(bufferedIn)) {

                            // read until we're out of data.
                            while (!streamFinished) {
                                nextByte = in.read();

                                if (nextByte > -1) {
                                    baos.write(nextByte);
                                }

                                if (nextByte == -1) {
                                    // we ran out of data. This message is complete.
                                    data = baos.toByteArray();
                                    streamFinished = true;
                                } else if (buffer.addAndCompare((byte) nextByte)) {
                                    // we matched our delimiter. This message is complete. We want all of the bytes from the
                                    // underlying BAOS exception for the last 'delimiterBytes.length' bytes because we don't want
                                    // the delimiter itself to be sent.
                                    data = Arrays.copyOfRange(baos.getUnderlyingBuffer(), 0, baos.size() - delimiterBytes.length);
                                }

                                if (data != null) {
                                    // If the message has no data, ignore it.
                                    if (data.length != 0) {
                                        // either we ran out of data or we reached the end of the message.
                                        // Either way, create the message because it's ready to send.
                                        final String message = new String(data, charset);

                                        // Add the message to the list of messages ready to send. If we've reached our
                                        // threshold of how many we're willing to send (or if we're out of data), go ahead
                                        // and send the whole List.
                                        messages.add(message);
                                        messageBytes += data.length;
                                        // TODO: add other batching strategies?
                                        if (messageBytes >= maxBufferSize || streamFinished) {
                                            // send the messages, then reset our state.
                                            try {
                                                for (String msg : messages) {
                                                    connection.publish(topic, msg);
                                                }
                                            } catch (final Exception e) {
                                                // we wrap the general exception in ProcessException because we want to separate
                                                // failures in sending messages from general Exceptions that would indicate bugs
                                                // in the Processor. Failure to send a message should be handled appropriately, but
                                                // we don't want to catch the general Exception or RuntimeException in order to catch
                                                // failures from NATS's Connection.
                                                throw new ProcessException("Failed to send messages to NATS", e);
                                            }

                                            messagesSent.addAndGet(messages.size());    // count number of messages sent

                                            // reset state
                                            messages.clear();
                                            messageBytes = 0;

                                            // We've successfully sent a batch of messages. Keep track of the byte offset in the
                                            // FlowFile of the last successfully sent message. This way, if the messages cannot
                                            // all be successfully sent, we know where to split off the data. This allows us to then
                                            // split off the first X number of bytes and send to 'success' and then split off the rest
                                            // and send them to 'failure'.
                                            lastMessageOffset.set(in.getBytesConsumed());
                                        }
                                    }
                                    // reset BAOS so that we can start a new message.
                                    baos.reset();
                                    data = null;

                                }
                            }

                            // If there are messages left, send them
                            // TODO: add other batching strategies?
                            if (!messages.isEmpty()) {
                                try {
                                    for (String msg : messages) {
                                        connection.publish(topic, msg);
                                    }
                                    messagesSent.addAndGet(messages.size()); // add count of messages
                                } catch (final Exception e) {
                                    throw new ProcessException("Failed to send messages to NATS", e);
                                }
                            }
                        }
                    }
                });

                final long nanos = System.nanoTime() - start;
                session.getProvenanceReporter().send(flowFile, "nats://" + topic, "Sent " + messagesSent.get() + " messages");
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().info("Successfully sent {} messages to NATS for {} in {} millis", new Object[] { messagesSent.get(), flowFile, TimeUnit.NANOSECONDS.toMillis(nanos) });
            } catch (final ProcessException pe) {
                error = true;

                // There was a failure sending messages to NATS. Iff the lastMessageOffset is 0, then all of them failed and we can
                // just route the FlowFile to failure. Otherwise, some messages were successful, so split them off and send them to
                // 'success' while we send the others to 'failure'.
                final long offset = lastMessageOffset.get();
                if (offset == 0L) {
                    // all of the messages failed to send. Route FlowFile to failure
                    getLogger().error("Failed to send {} to NATS due to {}; routing to fialure", new Object[] { flowFile, pe.getCause() });
                    session.transfer(flowFile, REL_FAILURE);
                } else {
                    // Some of the messages were sent successfully. We want to split off the successful messages from the failed messages.
                    final FlowFile successfulMessages = session.clone(flowFile, 0L, offset);
                    final FlowFile failedMessages = session.clone(flowFile, offset, flowFile.getSize() - offset);

                    getLogger().error("Successfully sent {} of the messages from {} but then failed to send the rest. Original FlowFile split into"
                        + " two: {} routed to 'success', {} routed to 'failure'. Failure was due to {}", new Object[] {
                        messagesSent.get(), flowFile, successfulMessages, failedMessages, pe.getCause() });

                    session.transfer(successfulMessages, REL_SUCCESS);
                    session.transfer(failedMessages, REL_FAILURE);
                    session.remove(flowFile);
                    session.getProvenanceReporter().send(successfulMessages, "nats://" + topic);
                }
            } finally {
                if (error) {
                    closeWithLogging(connection);
                } else {
                    returnConnection(connection);
                }
            }

        }
    }

}
