package be.vito.eodata.geopysparkextensions
import java.security.PrivilegedAction
import java.util.concurrent.TimeUnit

import geotrellis.store.accumulo.AccumuloInstance
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.impl.DelegationTokenImpl
import org.apache.accumulo.core.client.security.tokens.KerberosToken
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SparkConf

class AccumuloDelegationTokenProvider {
  def serviceName: String = "accumulo"


  /**
    * Generate delegation tokens, which Spark will pass on to the UserGroupInformation on each executor
    *
    * @param hadoopConf
    * @param sparkConf
    * @param creds
    * @return
    */
  def obtainCredentials(hadoopConf: Configuration, sparkConf: SparkConf, creds: Credentials): Option[Long] = {
    val useKerberos = ClientConfiguration
      .loadDefault()
      .getBoolean(ClientConfiguration.ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey, false)

    println("Obtaining accumulo creds")
    if (useKerberos) {
      def setupToken = new PrivilegedAction[Long] {
        override def run():Long = {
          val token = new KerberosToken()
          import org.apache.accumulo.core.client.admin.DelegationTokenConfig

          val accumulo = AccumuloInstance(
            "hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181",
            token.getPrincipal(),
            token)
          val config = new DelegationTokenConfig
          config.setTokenLifetime(6, TimeUnit.HOURS)
          val delegationToken: DelegationTokenImpl = accumulo.connector.securityOperations.getDelegationToken(config).asInstanceOf[DelegationTokenImpl]

          val identifier = delegationToken.getIdentifier
          val hadoopToken = new Token(identifier.getBytes, delegationToken.getPassword, identifier.getKind, delegationToken.getServiceName)
          creds.addToken(delegationToken.getServiceName, hadoopToken)
          println("Created token for: " + delegationToken.getServiceName.toString + " , expires: " + identifier.getExpirationDate.toString)
          //expiration date is provided in milliseconds
          identifier.getExpirationDate
        }
      }
      if (UserGroupInformation.getCurrentUser.hasKerberosCredentials) {
        return Some(setupToken.run())
      } else if (UserGroupInformation.getLoginUser.hasKerberosCredentials) {
        return Some(UserGroupInformation.getLoginUser.doAs[Long](setupToken))
      } else {
        throw new RuntimeException("No Kerberos credentials to log in to Accumulo found, please log in first.")
      }

    }

    None
  }
}
