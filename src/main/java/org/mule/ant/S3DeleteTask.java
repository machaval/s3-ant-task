/**
 *
 */
package org.mule.ant;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;

public class S3DeleteTask extends AWSTask
{


    /**
     * Target bucket, to which files should be uploaded.
     */
    private String bucket;

    /**
     * Destination dir on the S3 to which files should be deleted.
     */
    private String dir;


    private String endPoint = "s3-eu-west-1.amazonaws.com";


    /**
     * Executes the task.
     *
     * @see org.apache.tools.ant.Task#execute()
     */
    public void execute()
    {
        validateConfiguration();
        final AWSCredentials credential = new BasicAWSCredentials(getKey(), getSecret());
        final TransferManager transferManager = new TransferManager(credential);
        log(String.format("Region %s provided", getEndPoint()), Project.MSG_INFO);
        final AmazonS3 s3Client = transferManager.getAmazonS3Client();
        s3Client.setEndpoint(getEndPoint());
        if (getDir() != null && !getDir().isEmpty())
        {
            final ObjectListing objects = s3Client.listObjects(bucket, getDir());
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries())
            {
                s3Client.deleteObject(bucket, objectSummary.getKey());
                log(String.format("Key %s deleting successfully from %s bucket", objectSummary.getKey(), bucket), Project.MSG_INFO);
            }
        }
        log(String.format("Key %s deleted successfully from %s bucket", getDir(), bucket), Project.MSG_INFO);

    }


    private void validateConfiguration()
    {
        if (bucket == null)
        {
            throw new BuildException("Target bucket not given. Cannot upload");
        }
    }


    /**
     * ===============================================
     * Getters and setters
     * ===============================================
     */

    public String getDir()
    {
        return dir;
    }

    public void setDir(String dir)
    {
        this.dir = dir;
    }

    public void setBucket(String bucket)
    {
        this.bucket = bucket;
    }


    public String getEndPoint()
    {
        return endPoint;
    }

    public void setEndPoint(String endPoint)
    {
        this.endPoint = endPoint;
    }

}
