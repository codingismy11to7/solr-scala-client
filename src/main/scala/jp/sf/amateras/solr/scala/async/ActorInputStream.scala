package jp.sf.amateras.solr.scala.async

import java.io.InputStream

import jp.sf.amateras.solr.scala.async.ActorInputStream._

import akka.actor._
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * @author steven
 *
 */
object ActorInputStream {
    private case object Finish
    private case class GetBytes(max: Int)
    private case class GotBytes(bytes: Vector[Byte], eof: Boolean)
    private case class Write(bytes: Array[Byte])

    private class ISAct extends Actor with Stash {
        import context.dispatcher

        private var buffer = Vector.empty[Byte]
        private var finished = false
        private var hardShutdown = Option.empty[Cancellable]

        override def preStart() = {
            super.preStart()

            hardShutdown = Some(context.system.scheduler.scheduleOnce(3.minutes, self, PoisonPill))
        }

        override def postStop(): Unit = {
            super.postStop()

            hardShutdown foreach (_.cancel())
        }

        def receive = {
            case Finish ⇒
                finished = true
                unstashAll()

            case GetBytes(num) ⇒
                if (finished && buffer.isEmpty)
                    sender() ! GotBytes(buffer, eof = true)
                else if (buffer.isEmpty)
                    stash()
                else {
                    sender() ! GotBytes(buffer take num, eof = false)
                    buffer = buffer drop num
                }

            case Write(bytes) ⇒
                buffer ++= bytes
                unstashAll()
        }
    }
}

class ActorInputStream(arf: ActorRefFactory) extends InputStream {
    import akka.pattern._
    import scala.concurrent.duration._

    private val actor = arf actorOf Props[ISAct]

    def done() = actor ! Finish

    def write(bytes: Array[Byte]) = actor ! Write(bytes)

    override def close(): Unit = arf stop actor

    override def read(): Int = {
        implicit val timeout = Timeout(1.minute)
        val res = (actor ? GetBytes(1)).mapTo[GotBytes]
        val gb = Await.result(res, Duration.Inf)

        if (gb.eof) -1 else gb.bytes.head
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
        if (b == null)
            throw new NullPointerException
        else if (off < 0 || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException
        else if (len == 0)
            0
        else {
            implicit val timeout = Timeout(1.minute)
            val res = Await.result((actor ? GetBytes(len)).mapTo[GotBytes], Duration.Inf)

            if (res.eof)
                -1
            else {
                val ret = res.bytes.size
                res.bytes.zipWithIndex foreach {
                    case (byte, i) ⇒ b(i + off) = byte
                }
                ret
            }
        }
    }
}
