package org.openeo.geotrellissentinelhub

import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.http.{JavaClient, NoOpHttpClientConfigCallback}
import org.apache.http.client.config.RequestConfig
import org.elasticsearch.client.RestClientBuilder

object Elasticsearch {
  def client(uri: String): ElasticClient = ElasticClient(JavaClient(
    ElasticProperties(uri),
    new RestClientBuilder.RequestConfigCallback {
      override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder = {
        // for future reference:
        // * connectTimeout default (specify -1) = system default (tcp_syn_retries 6 corresponds to a total of 127s)
        // * socketTimeout default (specify -1) = system default (~ SO_TIMEOUT which defaults to 0 = infinite)

        // debugging shows that connectTimeout and socketTimeout are set internally to 1_000 and 30_000, respectively
        requestConfigBuilder
          .setConnectTimeout(10000)
      }
    },
    NoOpHttpClientConfigCallback
  ))
}
