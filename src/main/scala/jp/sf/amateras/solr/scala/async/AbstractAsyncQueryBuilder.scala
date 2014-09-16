package jp.sf.amateras.solr.scala.async

import jp.sf.amateras.solr.scala.async.AbstractAsyncQueryBuilder.OurStreamingCb
import jp.sf.amateras.solr.scala.query.{ExpressionParser, QueryTemplate}
import jp.sf.amateras.solr.scala.{QueryBuilderBase, CaseClassMapper, CaseClassQueryResult, MapQueryResult}
import org.apache.solr.client.solrj.StreamingResponseCallback
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.params.SolrParams
import akka.actor.ActorRefFactory
import scala.concurrent.Future

object AbstractAsyncQueryBuilder {

    abstract class OurStreamingCb extends StreamingResponseCallback {
        def errorReceived(t: Throwable): Unit
    }

}

abstract class AbstractAsyncQueryBuilder(query: String)(implicit parser: ExpressionParser)
    extends QueryBuilderBase[AbstractAsyncQueryBuilder] {

    def getResultAsMap(params: Any = null): Future[MapQueryResult] = {
        solrQuery.setQuery(new QueryTemplate(query).merge(CaseClassMapper.toMap(params)))
        query(solrQuery, { response => responseToMap(response) })
    }

    def getResultAs[T](params: Any = null)(implicit m: Manifest[T]): Future[CaseClassQueryResult[T]] = {
        solrQuery.setQuery(new QueryTemplate(query).merge(CaseClassMapper.toMap(params)))
        query(solrQuery, { response => responseToObject[T](response) })
    }

    def streamResults(cb: OurStreamingCb, arf: ActorRefFactory, params: Any = null) = {
        solrQuery.setQuery(new QueryTemplate(query).merge(CaseClassMapper.toMap(params)))
        stream(solrQuery, cb, arf)
    }

    protected def query[T](solrQuery: SolrParams, success: QueryResponse => T): Future[T]

    protected def stream(q: SolrParams, cb: OurStreamingCb, arf: ActorRefFactory): Unit
}
