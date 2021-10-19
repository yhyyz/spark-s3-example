import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Example {

  def main(args: Array[String]): Unit = {

    val ak ="****"
    val sk = "*****"

    // 如果没有ak,sk。使用Credential就取用下面的方法拿token，并使用其中ak,sk
    /**
     登陆到EMR master，执行如下命令
     curl http://169.254.169.254/latest/meta-data/iam/security-credentials/
     上面命令会返回一个Role，让后带上这个返回的Role，比如上边返回的是 EMR_EC2_DefaultRole， 执行如下命令
     curl http://169.254.169.254/latest/meta-data/iam/security-credentials/EMR_EC2_DefaultRole
     */

    // val token = "xxxx"

    val conf = new SparkConf()
      .setAppName("Example S3")
      .setMaster("local[3]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.cbo.enabled", "true")
      .set("fs.s3a.access.key", ak )
      .set("fs.s3a.secret.key", sk)
    // 如果使用Credential就打开下面配置
//      .set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
//      .set("fs.s3a.session.token", token)
    val spark = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    // 读S3,注意本地调试使用s3a路径，不要使用s3
    val df = spark.read.text("s3a://app-util/test-hupo.json")
    df.show()
  }

}
