package com.github.nifiedi.processors.datasonnet;

import com.datasonnet.document.MediaTypes;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Chaojun.Xu
 * @date 2024/11/27 17:31
 */


public class ExecuteDatasonnetScriptTest {
    private TestRunner runner = TestRunners.newTestRunner(ExecuteDatasonnetScript.class);

    @Test
    public void json2json() {
        runner.setProperty("datasonnet-script-body", "{\n" +
                "       \"greetings\": payload.greetings,\n" +
                "       \"uid\": property.uid,\n" +
                "       \"uname\": attribute.uname,\n" +
                "     }");
        runner.setProperty("uid", "123-123");
        runner.setProperty("input-media-type", MediaTypes.APPLICATION_JSON_VALUE);
        runner.setProperty("output-media-type", MediaTypes.APPLICATION_JSON_VALUE);
        runner.setProperty("read-from-attribute", "true");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("uname", "mapping");


        String payload = "{ \"greetings\": \"HelloWorld\"}";

        runner.enqueue(payload, attributes);
        runner.run();
        List<MockFlowFile> output = runner.getFlowFilesForRelationship("success");
        Relationship expectedRel = ExecuteDatasonnetScript.REL_SUCCESS;
        runner.assertTransferCount(expectedRel, 1);
        MockFlowFile mockFlowFile = output.get(0);
        String outputStr = new String(mockFlowFile.getContent());
        //System.out.println(outputStr);
        assert outputStr.equals("{\"greetings\":\"HelloWorld\",\"uid\":\"123-123\",\"uname\":\"mapping\"}");
    }

    @Test
    public void xml2json() {
        runner.setProperty("datasonnet-script-body", "payload");
        runner.setProperty("input-media-type", MediaTypes.APPLICATION_XML_VALUE);
        runner.setProperty("output-media-type", MediaTypes.APPLICATION_JSON_VALUE);

        String payload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<test:root xmlns:test=\"http://www.modusbox.com\">\n" +
                "    <test:mapping version=\"1.0\">Hello World</test:mapping>\n" +
                "</test:root>";
        runner.enqueue(payload);
        runner.run();
        List<MockFlowFile> output = runner.getFlowFilesForRelationship("success");
        Relationship expectedRel = ExecuteDatasonnetScript.REL_SUCCESS;
        runner.assertTransferCount(expectedRel, 1);
        MockFlowFile mockFlowFile = output.get(0);
        String outputStr = mockFlowFile.getContent();
        assert outputStr.equals("{\"test:root\":{\"@xmlns\":{\"test\":\"http://www.modusbox.com\"},\"test:mapping\":{\"@version\":\"1.0\",\"$\":\"Hello World\",\"~\":1},\"~\":1}}");

    }
}