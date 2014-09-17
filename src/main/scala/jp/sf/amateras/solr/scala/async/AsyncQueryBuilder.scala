package jp.sf.amateras.solr.scala.async

import AsyncUtils._
import com.ning.http.client.AsyncHandler.STATE
import com.ning.http.client._
import jp.sf.amateras.solr.scala.async.AbstractAsyncQueryBuilder.OurStreamingCb
import jp.sf.amateras.solr.scala.query._
import org.apache.http.HttpStatus
import org.apache.solr.client.solrj.impl.{StreamingBinaryResponseParser, XMLResponseParser}
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.util.ClientUtils
import org.apache.solr.common.params.{CommonParams, ModifiableSolrParams, SolrParams}
import akka.actor.ActorRefFactory
import scala.concurrent._
import scala.util.control.Exception.ultimately

class AsyncQueryBuilder(httpClient: AsyncHttpClient, url: String, protected val query: String)
                       (implicit parser: ExpressionParser) extends AbstractAsyncQueryBuilder(query) {
    private def postQueryBody(params: SolrParams) = {
        val qStr = ClientUtils.toQueryString(params, false)
        (if (qStr.charAt(0) == '?') qStr drop 1 else qStr) getBytes "UTF-8"
    }

    protected def createCopy = new AsyncQueryBuilder(httpClient, url, query)(parser)

    protected def query[T](solrQuery: SolrParams, success: QueryResponse => T): Future[T] = {
    val promise = Promise[T]()

    val wParams = new ModifiableSolrParams(solrQuery)
    wParams.set(CommonParams.WT, "xml")

    httpClient.preparePost(url + "/select")
      .setHeader("Content-Type", "application/x-www-form-urlencoded")
      .setBody(postQueryBody(wParams))
      .execute(new CallbackHandler(httpClient, promise, (response: Response) => {
          val parser = new XMLResponseParser
          val namedList = parser.processResponse(response.getResponseBodyAsStream, "UTF-8")
          val queryResponse = new QueryResponse
          queryResponse.setResponse(namedList)
          success(queryResponse)
        }))
    
    promise.future
  }

    protected def stream(q: SolrParams, cb: OurStreamingCb, arf: ActorRefFactory): Future[QueryResponse] = {
        val parser = new StreamingBinaryResponseParser(cb)
        val wParams = new ModifiableSolrParams(q)
        wParams.set(CommonParams.WT, parser.getWriterType)
        wParams.set(CommonParams.VERSION, parser.getVersion)
        val reqBody = postQueryBody(wParams)
        val reqBuilder = httpClient preparePost s"$url/select" setHeader
            ("Content-Type", "application/x-www-form-urlencoded") setBody reqBody
        val p = Promise[QueryResponse]()
        reqBuilder.execute(new AsyncHandler[Unit] {
            val ais = new UpdatableInputStream

            override def onThrowable(t: Throwable): Unit = cb.errorReceived(t)

            override def onCompleted(): Unit = {
                ais.done()
            }

            override def onStatusReceived(responseStatus: HttpResponseStatus): STATE = {
                responseStatus.getStatusCode match {
                    case HttpStatus.SC_OK ⇒
                        import arf.dispatcher
                        Future(blocking {
                            ultimately(ais.close()){
                                p success new QueryResponse(parser.processResponse(ais, null), null)
                            }
                        }) onFailure {
                            case t ⇒ p failure t
                        }
                        STATE.CONTINUE
                    case s ⇒
                        ais.close()
                        cb.errorReceived(new Exception(s"Non-OK response received: $s"))
                        STATE.ABORT
                }
            }

            override def onHeadersReceived(headers: HttpResponseHeaders): STATE = STATE.CONTINUE

            override def onBodyPartReceived(bodyPart: HttpResponseBodyPart): STATE = {
                ais write bodyPart.getBodyPartBytes
                STATE.CONTINUE
            }
        })

        p.future
    }
}
