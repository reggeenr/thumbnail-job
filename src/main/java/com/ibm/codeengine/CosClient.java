package com.ibm.codeengine;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.model.GetObjectRequest;
import com.ibm.cloud.objectstorage.services.s3.model.ListObjectsRequest;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectListing;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectResult;
import com.ibm.cloud.objectstorage.services.s3.model.S3ObjectSummary;

public class CosClient {

    private AmazonS3 cosClient;

    public CosClient(String apiKey, String serviceInstanceId) {

        String endpointUrl = "https://s3.eu-de.cloud-object-storage.appdomain.cloud";
        String location = "eu-de";
        System.out.println("COS enpoint URL: " + endpointUrl + ", location: " + location);
        System.out.println("APIKEY: " + (apiKey != null ? "****" : "NOT set"));
        System.out.println("RESOURCE_INSTANCE_ID: " + serviceInstanceId);

        this.cosClient = createClient(apiKey, serviceInstanceId, endpointUrl, location);
    }

    /**
     * @param apiKey
     * @param serviceInstanceId
     * @param endpointUrl
     * @return AmazonS3
     */
    public AmazonS3 createClient(String apiKey, String serviceInstanceId, String endpointUrl, String location) {
        AWSCredentials credentials = new BasicIBMOAuthCredentials(apiKey, serviceInstanceId);

        ClientConfiguration clientConfig = new ClientConfiguration().withRequestTimeout(5000);
        clientConfig.setUseTcpKeepAlive(true);

        AmazonS3 cosClient = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new EndpointConfiguration(endpointUrl, location))
                .withPathStyleAccessEnabled(true).withClientConfiguration(clientConfig).build();
        return cosClient;
    }

    /**
     * @param bucketName
     */
    public List<String> getObjectKeys(String bucketName, String prefix) {
        ObjectListing objectListing = cosClient.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix));

        List<String> objectKeys = new ArrayList<String>();
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            objectKeys.add(objectSummary.getKey());
        }
        return objectKeys;
    }


    /**
     * @param bucketName
     * @param key
     * @return the absolute path of the downloaded file
     * @throws Exception
     */
    public String downloadObject(String bucketName, String key) throws Exception {
        File targetFile =  new File("temp/"+key);
        cosClient.getObject(new GetObjectRequest(bucketName, key), targetFile);

        return targetFile.getAbsolutePath();
    }

    /**
     * 
     * @param bucketName
     * @param key
     * @param fileToUpload
     * @return
     * @throws Exception
     */
    public String uploadObject(String bucketName, String key, File fileToUpload) throws Exception {
        PutObjectResult result = cosClient.putObject(bucketName, key, fileToUpload);
        return result.getMetadata().getContentMD5();
    }

}
