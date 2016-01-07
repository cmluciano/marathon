package mesosphere.marathon.core.task.update.impl

import javax.inject.{ Inject, Named }

import com.google.inject.name.Names
import mesosphere.marathon.core.task.update.TaskStatusUpdateProcessor
import mesosphere.util.RestrictParallelExecutions
import org.apache.mesos.Protos.TaskStatus

import scala.concurrent.Future

object ThrottlingTaskStatusUpdateProcessor {
  final val name = "ThrottlingTaskStatusUpdateProcessor"
  lazy val named = Names.named(name)
}

private[core] class ThrottlingTaskStatusUpdateProcessor @Inject() (
  @Named(ThrottlingTaskStatusUpdateProcessor.name) serializePublish: RestrictParallelExecutions,
  @Named(ThrottlingTaskStatusUpdateProcessor.name) wrapped: TaskStatusUpdateProcessor)
    extends TaskStatusUpdateProcessor {
  override def publish(status: TaskStatus): Future[Unit] = serializePublish(wrapped.publish(status))
}
