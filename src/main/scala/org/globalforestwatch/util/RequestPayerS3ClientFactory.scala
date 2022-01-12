package org.globalforestwatch.util

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory
import org.apache.log4j.Logger


class RequestPayerS3ClientFactory extends DefaultS3ClientFactory {
  /**
    * Set AWS S3 Request Payer to requester
    * Only use this factory when using app outside of EMR and when using Hadoop < 3.4
    *
    * Usage:
    *
    * In code:
    * spark.sparkContext.hadoopConfiguration.set("fs.s3a.s3.client.factory.impl", "org.globalforestwatch.util.RequestPayerS3ClientFactory")
    *
    * Dynamically on runtime:
    * $SPARK_HOME/bin/spark-submit
    * --conf "spark.hadoop.fs.s3a.s3.client.factory.impl=org.globalforestwatch.util.RequestPayerS3ClientFactory"
    *
    * https://issues.apache.org/jira/browse/HADOOP-14661?focusedCommentId=17234612&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-17234612
    */

  override protected def newAmazonS3Client(credentials: AWSCredentialsProvider, awsConf: ClientConfiguration): AmazonS3 = {
    val logger = Logger.getLogger("RequestPayerS3ClientFactory")
    logger.info("Add Requester Pays header to all S3 requests")

    awsConf.addHeader("x-amz-request-payer", "requester");

    AmazonS3ClientBuilder.standard().withCredentials(credentials).withClientConfiguration(awsConf).build()
  }
}
