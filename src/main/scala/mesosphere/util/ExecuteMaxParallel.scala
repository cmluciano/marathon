package mesosphere.util

import javax.annotation.PreDestroy

import akka.actor._
import akka.pattern.pipe
import mesosphere.util.RestrictParallelExecutionsActor.Finished
import org.slf4j.LoggerFactory

import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.util.control.NonFatal

/**
  * Allows the sequential execution of methods which return `scala.concurrent.Future`s.
  * The execution of a `scala.concurrent.Future` waits for the prior Future to complete (not only the
  * method returning the Future).
  *
  * {{{
  * scala> import mesosphere.util.SerializeExecution
  * scala> val serializeExecution = SerializeExecution(someActorRef, "serialize")
  * scala> def myFutureReturningFunc: Future[Int] = Future.successful(1)
  * scala> val result: Future[Int] = serializeExecution(myFutureReturningFunc)
  * }}}
  */
object RestrictParallelExecutions {
  private val log = LoggerFactory.getLogger(getClass.getName)

  def apply[T](actorRefFactory: ActorRefFactory, actorName: String, maxParallel: Int): RestrictParallelExecutions = {
    new RestrictParallelExecutions(actorRefFactory, actorName, maxParallel)
  }
}

class RestrictParallelExecutions private (actorRefFactory: ActorRefFactory, actorName: String, maxParallel: Int) {
  import RestrictParallelExecutions.log

  private[this] val serializeExecutionActorProps = RestrictParallelExecutionsActor.props(maxParallel)
  private[this] val serializeExecutionActorRef = actorRefFactory.actorOf(serializeExecutionActorProps, actorName)

  def apply[T](block: => Future[T]): Future[T] = {
    PromiseActor.askWithoutTimeout(
      actorRefFactory, serializeExecutionActorRef, RestrictParallelExecutionsActor.Execute(() => block))
  }

  @PreDestroy
  def close(): Unit = {
    log.debug(s"stopping $serializeExecutionActorRef")
    serializeExecutionActorRef ! PoisonPill
  }
}

/**
  * Accepts messages containing functions returning `scala.concurrent.Future`s.
  * It starts the execution of a function after the `scala.concurrent.Future`s generated
  * by prior functions have been completed.
  */
private[util] class RestrictParallelExecutionsActor(maxParallel: Int) extends Actor {

  import RestrictParallelExecutionsActor.{ log, Execute, Queued }

  private[this] var active: Int = 0
  private[this] var queue: Queue[Queued] = Queue.empty

  override def preStart(): Unit = {
    super.preStart()

    for (execute <- queue) {
      execute.sender ! Status.Failure(new IllegalStateException(s"$self actor stopped"))
    }
  }

  override def receive: Receive = {
    case Execute(func) =>
      queue :+= Queued(sender(), func)
      startNextIfPossible()

    case Finished =>
      active -= 1
      startNextIfPossible()
  }

  private[this] def startNextIfPossible(): Unit = {
    if (active < maxParallel) {
      startNext()
    }
  }

  private[this] def startNext(): Unit = {
    queue.dequeueOption.foreach {
      case (next, newQueue) =>
        queue = newQueue
        active += 1

        import context.dispatcher
        import akka.pattern.pipe
        val future: Future[Any] = next.func()
        future.pipeTo(next.sender)
        future.onComplete(_ => self ! Finished)
    }
  }
}

private[util] object RestrictParallelExecutionsActor {
  def props(maxParallel: Int): Props = Props(new RestrictParallelExecutionsActor(maxParallel))

  private val log = LoggerFactory.getLogger(getClass.getName)
  case class Execute[T](func: () => Future[T])
  private case class Queued(sender: ActorRef, func: () => Future[_])
  private case object Finished
}
