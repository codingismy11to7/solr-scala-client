package jp.sf.amateras.solr.scala.async

import java.io.InputStream
import java.util.concurrent.atomic.AtomicBoolean

import akka.agent.Agent
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * @author steven
 *
 */
class AgentInputStream extends InputStream {
    import scala.concurrent.ExecutionContext.Implicits.global

    private val recvd = Agent(Vector.empty[Byte])
    private val finished = new AtomicBoolean(false)

    def done() = {
        finished.set(false)
        this.synchronized(this.notifyAll())
    }

    def write(bytes: Array[Byte]) = {
        recvd send (_ ++ bytes)
        this.synchronized(this.notifyAll())
    }

    private def allRecvd = Await.result(recvd.future(), Duration.Inf)

    private def bytes: Option[Vector[Byte]] = {
        while (allRecvd.isEmpty && !finished.get())
            this.synchronized(this.wait())
        if (recvd().nonEmpty)
            Some(recvd())
        else
            None
    }

    override def read(): Int = {
        bytes match {
            case None ⇒ -1
            case Some(bs) ⇒ 
                val ret = bs.head
                recvd send (_ drop 1)
                ret
        }
    }

    override def read(dest: Array[Byte], off: Int, len: Int): Int = {
        if (dest == null)
            throw new NullPointerException
        else if (off < 0 || len < 0 || len > dest.length - off)
            throw new IndexOutOfBoundsException
        else if (len == 0)
            0
        else {
            bytes match {
                case None ⇒ -1
                case Some(bs) ⇒ 
                    val bytes = bs take len
                    recvd send (_ drop bytes.size)
                    for (i ← 0 until bytes.size)
                        dest(i + off) = bytes(i)
                    bytes.size
            }
        }
    }
}
