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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.nats.Connection;

/**
 *
 * @author Matthew Ring
 */
public abstract class AbstractNatsProcessor extends AbstractProcessor {

    private static final String SINGLE_BROKER_REGEX = ".*?\\:\\d{3,5}";
    private static final String BROKER_REGEX = SINGLE_BROKER_REGEX + "(?:,\\s*" + SINGLE_BROKER_REGEX + ")*";
    
    public static final PropertyDescriptor SEED_BROKERS = new PropertyDescriptor.Builder()
        .name("Known Brokers")
        .description("A comma-separated list of known NATS Brokers in the format nats://<host>:<port>")
        .required(true)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile(BROKER_REGEX)))
        .expressionLanguageSupported(false)
        .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("The Character Set that should be used to encode the textual content of the NATS message")
        .required(true)
        .defaultValue("UTF-8")
        .allowableValues(Charset.availableCharsets().keySet().toArray(new String[0]))
        .build();
// TODO: future
//    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
//        .name("Communications Timeout")
//        .description("The amount of time to wait for a response from NATS before determining that there is a communications error")
//        .required(true)
//        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
//        .expressionLanguageSupported(false)
//        .defaultValue("30 secs")
//        .build();
    
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SEED_BROKERS);
        props.add(CHARSET);
// TODO: future
//        props.add(TIMEOUT);
        return props;
    }
    
    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> errors = new ArrayList<>(super.customValidate(context));

        return errors;
    }
    
    protected Properties createConfig(final ProcessContext context) {
        
        final String brokers = context.getProperty(SEED_BROKERS).getValue();
        
        final Properties properties = new Properties();
        properties.setProperty("uri", brokers);
        
// TODO: future
//        properties.setProperty("request.timeout.ms", String.valueOf(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).longValue()));
//        properties.setProperty("message.send.max.retries", "1");
//        properties.setProperty("request.required.acks", context.getProperty(DELIVERY_GUARANTEE).getValue());
//        properties.setProperty("producer.type", context.getProperty(PRODUCER_TYPE).getValue());

        return properties;
    }

    protected Connection createConnection(final ProcessContext context) {
        try {
            return Connection.connect(createConfig(context), null);
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    protected void closeWithLogging(Connection connection) {
        try {
            connection.close(true);
        } catch (IOException ex) {
            // TODO: more elaborate handling needed?
            getLogger().warn("Failed to close NATS connection.", ex);
        }
    }

}
