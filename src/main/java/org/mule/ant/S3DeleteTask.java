/**
 *
 */
package org.mule.ant;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;

public class S3DeleteTask extends AWSTask {


    /**
     * Target bucket, to which files should be uploaded.
     */
    private String bucket;

    /**
     * Destination dirs on the S3 to which files should be deleted.
     */
    private String dir;


    private String endPoint = "s3-eu-west-1.amazonaws.com";

    private List<S3Element> elements = new ArrayList<S3Element>();


    /**
     * Executes the task.
     *
     * @see org.apache.tools.ant.Task#execute()
     */
    public void execute() {
        validateConfiguration();
        final AWSCredentials credential = new BasicAWSCredentials(getKey(), getSecret());
        final TransferManager transferManager = new TransferManager(credential);
        log(String.format("Region %s provided", getEndPoint()), Project.MSG_INFO);
        final AmazonS3 s3Client = transferManager.getAmazonS3Client();
        s3Client.setEndpoint(getEndPoint());
        final DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket);
        List<ObjectListing> allObjects = initialListObjects(s3Client);
        for (ObjectListing object : allObjects) {
            ObjectListing objects = object;
            do {
                objects = listObjects(s3Client, objects);
                List<KeyVersion> keys = collectKeys(objects);
                if (!keys.isEmpty()) {
                    s3Client.deleteObjects(deleteObjectsRequest.withKeys(keys));
                    log(String.format("Batch of %s keys deleted from %s bucket", keys.size(), bucket), Project.MSG_INFO);
                }
            } while (objects.isTruncated());
        }

    }

    private List<KeyVersion> collectKeys(ObjectListing objects) {
        List<KeyVersion> keys = new ArrayList<DeleteObjectsRequest.KeyVersion>();
        for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
            keys.add(new KeyVersion(objectSummary.getKey()));
            log(String.format("Adding key %s to be deleted from %s bucket", objectSummary.getKey(), bucket), Project.MSG_DEBUG);
        }
        return keys;
    }

    private ObjectListing listObjects(final AmazonS3 s3Client, ObjectListing objectListing) {
        return s3Client.listNextBatchOfObjects(objectListing);
    }

    private List<ObjectListing> initialListObjects(final AmazonS3 s3Client) {
        List<ObjectListing> result = new ArrayList<ObjectListing>();
        if (getDir() != null && !getDir().isEmpty()) {
            result.add(s3Client.listObjects(bucket, getDir()));
        }

        if (!elements.isEmpty()) {
            for (S3Element element : elements) {
                result.add(s3Client.listObjects(bucket, element.getName()));
            }
        }

        if (result.isEmpty()) {
            result.add(s3Client.listObjects(bucket));
        }
        return result;
    }

    private void validateConfiguration() {
        if (bucket == null) {
            throw new BuildException("Target bucket not given. Cannot upload");
        }
    }


    /**
     * ===============================================
     * Getters and setters
     * ===============================================
     */

    public void addElement(S3Element element) {
        elements.add(element);
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }


    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

}
