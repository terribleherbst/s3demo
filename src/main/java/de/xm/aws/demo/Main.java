package de.xm.aws.demo;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Collectors;

public class Main {

    static class AwsS3Access {

        AmazonS3 amazonS3;

        String bucketName = "vista.drymatter.cultivent.kwscloud.de";

        String awsApplicationRegion = "eu-central-1";

        AwsS3Access() {
            this.amazonS3 = createAwsClientWithDefaultAuthentication();
        }

        /**
         * Create an AWS client which is secured using the
         * default AWS security information
         * http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
         */
        private AmazonS3 createAwsClientWithDefaultAuthentication() {
            return AmazonS3ClientBuilder
                .standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(awsApplicationRegion)
                .build();
        }

        public void storeFileInBucket(String filename, Path data) {
            amazonS3.putObject(bucketName, filename, data.toFile());
        }

        public String readStringFromBucket(String filename) {
            S3Object s3Object = amazonS3.getObject(bucketName, filename);

            String result = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))
                .lines()
                .parallel()
                .collect(Collectors.joining("\n"));
            return result;
        }

        public void deleteFileInBucket(String filename) {
            S3Object s3Object = amazonS3.getObject(bucketName, filename);
            amazonS3.deleteObject(bucketName, filename);
        }

    }

    public static void main(String[] args) throws IOException {
        AwsS3Access awsS3Access = new AwsS3Access();
        Path tempFile = Files.createTempFile("sample", "txt");
        Files.write(tempFile, Collections.singletonList("Das ist das Haus der KWS"));
        awsS3Access.storeFileInBucket("Beispiel.txt", tempFile);
        System.out.println(awsS3Access.readStringFromBucket("Beispiel.txt"));
        awsS3Access.deleteFileInBucket("Beispiel.txt");
    }
}
