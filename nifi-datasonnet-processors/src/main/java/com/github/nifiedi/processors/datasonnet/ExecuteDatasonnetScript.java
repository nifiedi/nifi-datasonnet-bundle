package com.github.nifiedi.processors.datasonnet;

import com.datasonnet.Mapper;
import com.datasonnet.MapperBuilder;
import com.datasonnet.document.DefaultDocument;
import com.datasonnet.document.Document;
import com.datasonnet.document.MediaTypes;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.github.nifiedi.processors.datasonnet.Utils.asFile;
import static com.github.nifiedi.processors.datasonnet.Utils.getMediaType;

/**
 * @author Chaojun.Xu
 * @date 2024/11/27 17:14
 */

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"datasonnet","data transformation"})
@CapabilityDescription("Datasonnet script processor. The script is responsible for handling the incoming flow file as well as any flow files created by the script. If the handling is incomplete or incorrect, the session will be rolled back")
@WritesAttribute(attribute = "mime.type", description = "change the mime.type as output-media-type")
@DynamicProperty(name = "datasonnet property", value = "dynamic property", expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
        description = "dynamic property, can be used in datasonnet script file")
public class ExecuteDatasonnetScript extends AbstractProcessor {
    public static final PropertyDescriptor SCRIPT_FILE = new PropertyDescriptor.Builder()
            .name("datasonnet-script-file")
            .displayName("Script File")
            .required(false)
            .description("Path to script file to execute. Only one of Script File or Script Body may be used")
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SCRIPT_BODY = new PropertyDescriptor.Builder()
            .name("datasonnet-script-body")
            .displayName("Script Body")
            .required(false)
            .description("Body of script to execute. Only one of Script File or Script Body may be used")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor INPUT_MEDIA_TYPE = new PropertyDescriptor.Builder()
            .name("input-media-type")
            .displayName("Input Media Type")
            .required(true)
            .description("Input media type.xml,json,csv,txt,etc")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(MediaTypes.APPLICATION_JSON_VALUE, MediaTypes.APPLICATION_XML_VALUE, MediaTypes.APPLICATION_YAML_VALUE, MediaTypes.APPLICATION_CSV_VALUE)
            .build();

    public static final PropertyDescriptor OUTPUT_MEDIA_TYPE = new PropertyDescriptor.Builder()
            .name("output-media-type")
            .displayName("Output Media Type")
            .required(true)
            .description("Input media type.xml,json,csv,txt,etc")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(MediaTypes.APPLICATION_JSON_VALUE, MediaTypes.APPLICATION_XML_VALUE, MediaTypes.APPLICATION_YAML_VALUE, MediaTypes.APPLICATION_CSV_VALUE)
            .build();

    public static final PropertyDescriptor READ_FROM_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("read-from-attribute")
            .displayName("Read From Attribute")
            .required(true)
            .defaultValue("false")
            .description("Read from attribute. Read variable from attribute. use attribute.attributeName to get the value in mapping script.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("FlowFiles that were successfully processed").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles that failed to be processed").build();
    private Mapper mapper;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    File scriptFile = null;  //SCRIPT_FILE
    String scriptBody = null; //SCRIPT_BODY

    String inputMediaType = null; //INPUT_MEDIA_TYPE
    String outputMediaType = null; //OUTPUT_MEDIA_TYPE
    String readFromAttribute = null; //READ_FROM_ATTRIBUTE

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SCRIPT_FILE);
        descriptors.add(SCRIPT_BODY);
        descriptors.add(INPUT_MEDIA_TYPE);
        descriptors.add(OUTPUT_MEDIA_TYPE);
        descriptors.add(READ_FROM_ATTRIBUTE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        HashSet<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        PropertyDescriptor.Builder propertyBuilder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .dynamic(true);

        return propertyBuilder
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

    }


    private Map<String, Document<?>> buildAttributesVariableForFlowFile(FlowFile ff, ProcessContext processContext) {
        Map<String, Document<?>> variables = new HashMap<>();

        if ("true".equals(readFromAttribute)) {
            variables.put("attribute", new DefaultDocument<>(ff.getAttributes(), MediaTypes.APPLICATION_JAVA));
        }
        Map<String, String> properties = new HashMap<>();

        for (final PropertyDescriptor descriptor : processContext.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                properties.put(descriptor.getName(), processContext.getProperty(descriptor).evaluateAttributeExpressions().getValue());
            }
        }
        variables.put("property", new DefaultDocument<>(properties, MediaTypes.APPLICATION_JAVA));
        return variables;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        this.scriptFile = asFile(context.getProperty(SCRIPT_FILE).evaluateAttributeExpressions().getValue());  //SCRIPT_FILE
        this.scriptBody = context.getProperty(SCRIPT_BODY).getValue(); //SCRIPT_BODY
        this.inputMediaType = context.getProperty(INPUT_MEDIA_TYPE).getValue(); //INPUT_MEDIA_TYPE
        this.outputMediaType = context.getProperty(OUTPUT_MEDIA_TYPE).getValue(); //OUTPUT_MEDIA_TYPE
        this.readFromAttribute = context.getProperty(READ_FROM_ATTRIBUTE).getValue(); //READ_FROM_ATTRIBUTE
        if (scriptBody != null && scriptFile != null) {
            throw new ProcessException("Only one parameter accepted: `" + SCRIPT_BODY.getDisplayName() + "` or `" + SCRIPT_FILE.getDisplayName() + "`");
        }
        if (scriptBody == null && scriptFile == null) {
            throw new ProcessException("At least one parameter required: `" + SCRIPT_BODY.getDisplayName() + "` or `" + SCRIPT_FILE.getDisplayName() + "`");
        }
        if (scriptFile != null) {
            scriptBody = FileUtils.readFileToString(scriptFile, "utf-8");
        }


    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);
        try {
            final FlowFile transformed = session.write(original, (inputStream, outputStream) -> {
                try (final InputStream bufferedInputStream = new BufferedInputStream(inputStream)) {
                    String payload = IOUtils.toString(bufferedInputStream, StandardCharsets.UTF_8);
                    Map<String, Document<?>> variables = buildAttributesVariableForFlowFile(original, processContext);
                    mapper = new MapperBuilder(scriptBody).withInputNames(variables.keySet()).build();
                    String output = mapper.transform(new DefaultDocument<>(payload, getMediaType(inputMediaType)), variables, getMediaType(outputMediaType)).getContent();
                    outputStream.write(output.getBytes(StandardCharsets.UTF_8));
                }
            });
            session.putAttribute(transformed, "mime.type", outputMediaType);
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            getLogger().info("Transformation Completed {}", original);

        } catch (Exception e) {
            getLogger().error("Execute mapping script error.", e);
            session.transfer(original, REL_FAILURE);
        }

    }
}