package mesosphere.marathon.upgrade

import akka.event.EventStream
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.task.tracker.TaskTracker
import mesosphere.marathon.state.PathId
import org.apache.mesos.Protos.TaskID
import org.apache.mesos.SchedulerDriver

import scala.collection.mutable
import scala.concurrent.Promise

class TaskKillActor(
    val driver: SchedulerDriver,
    val appId: PathId,
    val taskTracker: TaskTracker,
    val eventBus: EventStream,
    tasksToKill: Set[MarathonTask],
    val promise: Promise[Unit]) extends StoppingBehavior {

  var idsToKill = tasksToKill.map(_.getId).to[mutable.Set]

  def initializeStop(): Unit = {
    log.info(s"Killing ${tasksToKill.size} instances")
    for (task <- tasksToKill) {
      driver.killTask(taskId(task.getId))
    }
  }

  private def taskId(id: String) = TaskID.newBuilder().setValue(id).build()
}
