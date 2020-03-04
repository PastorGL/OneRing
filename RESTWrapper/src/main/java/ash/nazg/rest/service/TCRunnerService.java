/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import ash.nazg.config.tdl.TDLObjectMapper;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.xml.sax.InputSource;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

@Singleton
public class TCRunnerService extends Runner {
    private static final String TC_BUILD_XML = "<build>" +
            "<buildType id=\"{BUILD_TYPE}\"/>" +
            "<properties>" +
            "<property name=\"PARAMS\" value=\"{PARAMS}\"/>" +
            "<property name=\"TASKS_JSON_PATH\" value=\"{TASKS_JSON_PATH}\"/>" +
            "</properties>" +
            "</build>";

    private final String tcInstance;
    private final String tcUser;
    private final String tcPassword;
    private final String tcBuildType;

    private final String awsProfile;
    private final String s3Bucket;

    @Inject
    public TCRunnerService(Properties props) {
        this.tcInstance = props.getProperty("tc.instance");
        this.tcUser = props.getProperty("tc.user");
        this.tcPassword = props.getProperty("tc.password");
        this.tcBuildType = props.getProperty("tc.buildType");

        this.awsProfile = props.getProperty("aws.profile", "default");
        this.s3Bucket = props.getProperty("s3.bucket");
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
            String taskKey = "tasks/" + tcBuildType + "-" + new Date().getTime() + "-" + new Random().nextLong() + ".json";

            ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(awsProfile);
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(credentialsProvider)
                    .build();

            byte[] bytes = new TDLObjectMapper().writeValueAsBytes(task);
            ObjectMetadata om = new ObjectMetadata();
            om.setContentType("application/json");
            om.setContentLength(bytes.length);
            s3.putObject(new PutObjectRequest(s3Bucket,
                    taskKey,
                    new ByteArrayInputStream(bytes), om));

            // consider all build properties as XML-safe, no need to encode
            String props = TC_BUILD_XML.replace("{BUILD_TYPE}", tcBuildType)
                    .replace("{PARAMS}", params)
                    .replace("{TASKS_JSON_PATH}", "s3://" + s3Bucket + "/" + taskKey);

            Files.write(
                    new File(tempDir, "request.xml").toPath(),
                    props.getBytes(),
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW
            );

            executeAndWait(tempDir, "Can't trigger TC build", "curl",
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

    //Java 8 standard HTTP support is an utter unreliable nonsense. So we call curl instead.
    //TODO: Migrate to new HTTPClient when Java 11 or newer becomes target
    @Deprecated
    private static void executeAndWait(File tempDir, String errorMessage, String... command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(tempDir);
        Process p = pb.start();
        if (p.waitFor() > 0) {
            throw new InterruptedException(errorMessage);
        }
    }
}
