import com.amazonaws.services.s3._
import com.amazonaws.auth._
import java.io.File
import model._
import scala.collection.JavaConversions._

class S3 (val credentials:AWSCredentials, val defaultBucket:String,  val endpoint:String) {
  lazy val client = getClient
  
  def getClient = {
    val c = new AmazonS3Client(credentials)
    c.setEndpoint(endpoint)
    c
  }

  def enableVersioningStatus(bucket:Option[String]) {
    val versioning = new SetBucketVersioningConfigurationRequest(bucket.getOrElse(defaultBucket), new BucketVersioningConfiguration((BucketVersioningConfiguration.ENABLED)))
    client.setBucketVersioningConfiguration(versioning)
  }

  def suspendVersioningStatus(bucket:Option[String]) {
    val versioning = new SetBucketVersioningConfigurationRequest(bucket.getOrElse(defaultBucket), new BucketVersioningConfiguration((BucketVersioningConfiguration.SUSPENDED)))
    client.setBucketVersioningConfiguration(versioning)
  }

  def getVersioningStatus(bucket:Option[String]):String = {
    client.getBucketVersioningConfiguration(bucket.getOrElse(defaultBucket)).getStatus
  }
  
  def getVersions(bucket:Option[String]) = {
    val req = new ListVersionsRequest
    req.setBucketName(bucket.getOrElse(defaultBucket))
    val result = client.listVersions(req)
    result.getVersionSummaries
  }
  
  def clean(bucket:Option[String]) = {
    val versions = getVersions(bucket)
    versions.foreach (v => {
      client.deleteVersion(bucket.getOrElse(defaultBucket), v.getKey, v.getVersionId)
    })
  }
  
  def listKeys(mask:Option[String], bucket:Option[String]) = {
    val req = new ListObjectsRequest()
    req.setPrefix(mask.getOrElse(""))
    req.setBucketName(bucket.getOrElse(defaultBucket))
    client.listObjects(req)
  }
  
  def downloadFiles(basePath:String, keys:List[String], bucket:Option[String]) = {
     keys.foreach(key => {
       val req = new GetObjectRequest(bucket.getOrElse(defaultBucket), key)
       val file = new File(basePath + key)
       val path = new File(file.getParent)
       if (!path.exists) {
         path.mkdirs()
       }
       client.getObject(req, file)
     })
  }

  // no sube los directorios vacios
  def uploadFiles(baseKey:Option[String], file:File) {
    if (file.isDirectory) {
      file.listFiles.foreach(uploadFiles(baseKey, _))
    } else {
      val key = baseKey match {
        case Some(s) => s + file.getPath
        case None => file.getPath.tail
      }
      client.putObject(new PutObjectRequest(defaultBucket, key, file))
    }
  }
}

// objeto basico para acceder a S3
object S3 {
  def apply(bucket:String, endpoint:String, secret:String, access:String) = {
    val credentials = new AWSCredentials {
      def getAWSSecretKey: String = secret
      def getAWSAccessKeyId: String = access
    }

    new S3(credentials, bucket, endpoint)
  }
}

