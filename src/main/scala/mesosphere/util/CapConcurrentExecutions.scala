package mesosphere.util

import javax.annotation.PreDestroy

import akka.actor._
import akka.pattern.pipe
import mesosphere.marathon.metrics.Metrics.AtomicIntGauge
import mesosphere.marathon.metrics.{ MetricPrefixes, Metrics }
import mesosphere.util.RestrictParallelExecutionsActor.Finished
import org.slf4j.LoggerFactory

import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.util.Failure
import scala.util.control.NonFatal

/**
  * Allows capping parallel executions of methods which return `scala.concurrent.Future`s.
  * Only `maxParallel` concurrent executions are allowed.
  *
  * {{{
  * scala> import mesosphere.util.CapConcurrentExecutions
  * scala> val capParallelExecution = CapConcurrentExecutions(someActorRefFactory, "serialize")
  * scala> def myFutureReturningFunc: Future[Int] = Future.successful(1)
  * scala> val result: Future[Int] = capParallelExecution(myFutureReturningFunc)
  * }}}
  */
object CapConcurrentExecutions {
  private val log = LoggerFactory.getLogger(getClass.getName)

  def apply[T](
    metrics: CapConcurrentExecutionsMetrics,
    actorRefFactory: ActorRefFactory,
    actorName: String,
    maxParallel: Int,
    maxQueued: Int): CapConcurrentExecutions = {
    new CapConcurrentExecutions(metrics, actorRefFactory, actorName, maxParallel, maxQueued)
  }
}

class CapConcurrentExecutionsMetrics(metrics: Metrics, metricsClass: Class[_]) {
  val queued = metrics.gauge(metrics.name(MetricPrefixes.SERVICE, metricsClass, "queued"), new AtomicIntGauge)
  val processing = metrics.gauge(metrics.name(MetricPrefixes.SERVICE, metricsClass, "processing"), new AtomicIntGauge)
  val processingTimer = metrics.timer(metrics.name(MetricPrefixes.SERVICE, metricsClass, "processing-time"))

  def reset(): Unit = {
    queued.setValue(0)
    processing.setValue(0)
  }
}

class CapConcurrentExecutions private (
    metrics: CapConcurrentExecutionsMetrics,
    actorRefFactory: ActorRefFactory,
    actorName: String,
    maxParallel: Int,
    maxQueued: Int) {
  import CapConcurrentExecutions.log

  private[this] val serializeExecutionActorProps =
    RestrictParallelExecutionsActor.props(metrics, maxParallel = maxParallel, maxQueued = maxQueued)
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
  * Accepts execute instructions containing functions returning `scala.concurrent.Future`s.
  * It only allows `maxParallel` parallel executions and queues the other operations.
  * It will not queue more than `maxQueued` execute instructions.
  */
private[util] class RestrictParallelExecutionsActor(
    metrics: CapConcurrentExecutionsMetrics, maxParallel: Int, maxQueued: Int) extends Actor {

  import RestrictParallelExecutionsActor.{ log, Execute, Queued }

  private[this] var active: Int = 0
  private[this] var queue: Queue[Queued] = Queue.empty

  override def preStart(): Unit = {
    super.preStart()

    metrics.reset()

    for (execute <- queue) {
      execute.sender ! Status.Failure(new IllegalStateException(s"$self actor stopped"))
    }
  }

  override def postStop(): Unit = {
    metrics.reset()

    super.postStop()
  }

  override def receive: Receive = {
    case Execute(func) =>

      if (active >= maxParallel && queue.size >= maxQueued) {
        sender ! Status.Failure(new IllegalStateException(s"$self queue may not exceed $maxQueued entries"))
      }
      else {
        queue :+= Queued(sender(), func)
        startNextIfPossible()
      }

    case Finished =>
      active -= 1
      startNextIfPossible()
  }

  private[this] def startNextIfPossible(): Unit = {
    if (active < maxParallel) {
      startNext()
    }

    metrics.processing.setValue(active)
    metrics.queued.setValue(queue.size)
  }

  private[this] def startNext(): Unit = {
    queue.dequeueOption.foreach {
      case (next, newQueue) =>
        queue = newQueue
        active += 1

        import context.dispatcher
        import akka.pattern.pipe
        val future: Future[Any] =
          try metrics.processingTimer.timeFuture(next.func())
          catch { case NonFatal(e) => Future.failed(e) }
        future.pipeTo(next.sender)

        val myself = self
        future.onComplete(_ => myself ! Finished)(CallerThreadExecutionContext.callerThreadExecutionContext)
    }
  }
}

private[util] object RestrictParallelExecutionsActor {
  def props(metrics: CapConcurrentExecutionsMetrics, maxParallel: Int, maxQueued: Int): Props =
    Props(new RestrictParallelExecutionsActor(metrics, maxParallel = maxParallel, maxQueued = maxQueued))

  private val log = LoggerFactory.getLogger(getClass.getName)
  case class Execute[T](func: () => Future[T])
  private case class Queued(sender: ActorRef, func: () => Future[_])
  private case object Finished
}
