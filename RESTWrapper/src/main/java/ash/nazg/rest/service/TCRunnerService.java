package ash.nazg.rest.service;

import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.xml.sax.InputSource;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

@Singleton
public class TCRunnerService extends Runner {
    private static final String TC_BUILD_XML = "<build>" +
            "<buildType id=\"{BUILD_TYPE}\"/>" +
            "<properties>" +
            "<property name=\"PARAMS\" value=\"{PARAMS}\"/>" +
            "</properties>" +
            "</build>";

    private final String tcInstance;
    private final String tcUser;
    private final String tcPassword;
    private final String tcBuildType;

    @Inject
    public TCRunnerService(Properties props) {
        this.tcInstance = props.getProperty("tc.instance");
        this.tcUser = props.getProperty("tc.user");
        this.tcPassword = props.getProperty("tc.password");
        this.tcBuildType = props.getProperty("tc.buildType");
    }

    @Override
    public TaskStatus status(String taskId) throws Exception {
        File tempDir = makeTempDir();

        try {
            executeAndWait(tempDir, "Can't query TC build state", "curl",
                    "-u", tcUser + ":" + tcPassword,
                    tcInstance + taskId,
                    "-o", "./answer.xml");

            InputSource inputSource = new InputSource(new FileInputStream(new File(tempDir, "answer.xml")));
            XPath xpath = XPathFactory.newInstance().newXPath();

            String state = xpath.evaluate("//build/@state", inputSource);
            switch (state.toLowerCase()) {
                case "finished": {
                    inputSource = new InputSource(new FileInputStream(new File(tempDir, "answer.xml")));
                    if (xpath.evaluate("//build/@status", inputSource).equalsIgnoreCase("SUCCESS")) {
                        return TaskStatus.SUCCESS;
                    } else {
                        return TaskStatus.FAILURE;
                    }
                }
                case "running":
                    return TaskStatus.RUNNING;
            }

            return TaskStatus.QUEUED;
        } catch (XPathExpressionException | NullPointerException e) {
            return TaskStatus.NOT_FOUND;
        } finally {
            removeTempDir(tempDir);
        }
    }

    @Override
    public String define(TaskDefinitionLanguage.Task task, String params) throws Exception {
        File tempDir = makeTempDir();

        try {
            // consider all build properties as XML-safe, no need to encode
            String props = TC_BUILD_XML.replace("{BUILD_TYPE}", tcBuildType)
                    .replace("{PARAMS}", params);

            Files.write(
                    new File(tempDir, "request.xml").toPath(),
                    props.getBytes(),
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW
            );

            executeAndWait(
                    tempDir, "Can't trigger TC build", "curl",
                    "-u", tcUser + ":" + tcPassword,
                    tcInstance + "/httpAuth/app/rest/buildQueue",
                    "-H", "Content-Type:application/xml",
                    "--data-binary", "@./request.xml",
                    "-o", "./answer.xml"
            );

            XPath xpath = XPathFactory.newInstance().newXPath();
            InputSource inputSource = new InputSource(new FileInputStream(new File(tempDir, "answer.xml")));

            return xpath.evaluate("//build/@href", inputSource);
        } finally {
            removeTempDir(tempDir);
        }
    }
}
