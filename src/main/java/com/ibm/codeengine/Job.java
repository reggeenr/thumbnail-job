package com.ibm.codeengine;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Timestamp;
import java.util.List;

import javax.imageio.ImageIO;

import com.ibm.cloud.objectstorage.SDKGlobalConfiguration;

import net.coobird.thumbnailator.Thumbnails;

/**
 * See also
 * https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-java&locale=de
 */
public class Job {

    public static void main(String[] args) {
        SDKGlobalConfiguration.IAM_ENDPOINT = "https://iam.cloud.ibm.com/identity/token";

        String apiKey = System.getenv("CLOUD_OBJECT_STORAGE_APIKEY");
        String serviceInstanceId = System.getenv("CLOUD_OBJECT_STORAGE_RESOURCE_INSTANCE_ID");

        String bucketName = System.getenv("BUCKET");
        System.out.println("BUCKET: " + (bucketName != null ? bucketName : "NOT set"));

        String jobIndex = System.getenv("JOB_INDEX");
        System.out.println("JOB_INDEX: " + jobIndex);

        System.out.println("Current time: " + new Timestamp(System.currentTimeMillis()).toString());

        CosClient cos = new CosClient(apiKey, serviceInstanceId);
        List<String> images = cos.getObjectKeys(bucketName, jobIndex + "__");

        for (String image : images) {
            if (image.contains("-thumb")) {
                System.out.println("Skipping '" + image + "' as it already has a thumbnail");
                continue;
            }

            System.out.println("Processing " + image + " ...");

            try {
                // Download the object
                String targetPath = cos.downloadObject(bucketName, image);
                System.out.println("Downloaded to: " + targetPath);

                // Make sure that the same file extension is used for the thumbnail, too
                String fileExtension = image.substring(image.lastIndexOf('.'));
                String thumbnailSuffix = "-thumb" + fileExtension;
                System.out.println("thumbnailSuffix: " + thumbnailSuffix);

                // Create the Thumbnail
                Thumbnails.of(targetPath).size(50, 50).toFile(targetPath + thumbnailSuffix);

                // Upload the object to the bucket
                cos.uploadObject(bucketName, image + "-thumb", new File(targetPath + thumbnailSuffix));

                System.out.println("Processing " + image + " [done]");
            } catch (Exception e) {
                System.out.println("Failed to create a thumbnail for file " + image);
                e.printStackTrace();
                System.out.println("Processing " + image + " [failed]");
            }
        }
    }
}
