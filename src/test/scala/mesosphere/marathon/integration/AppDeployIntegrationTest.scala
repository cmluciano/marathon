package mesosphere.marathon.integration

import java.lang.{ Double => JDouble }

import mesosphere.marathon.api.v2.json.V2AppDefinition
import mesosphere.marathon.health.HealthCheck
import mesosphere.marathon.integration.setup._
import mesosphere.marathon.state._
import org.scalatest.{ BeforeAndAfter, GivenWhenThen, Matchers }
import org.slf4j.LoggerFactory
import play.api.libs.json.JsArray

import scala.concurrent.duration._
import scala.util.control.NonFatal

class AppDeployIntegrationTest
    extends IntegrationFunSuite
    with SingleMarathonIntegrationTest
    with Matchers
    with BeforeAndAfter
    with GivenWhenThen {

  private[this] val log = LoggerFactory.getLogger(getClass)

  //clean up state before running the test case
  before(cleanUp())

  test("deploy a simple Docker app") {
    Given("a new Docker app")
    val app = V2AppDefinition(
      AppDefinition(
        id = testBasePath / "dockerapp",
        cmd = Some("sleep 50000"),
        container = Some(
          Container(docker = Some(mesosphere.marathon.state.Container.Docker(
            image = "busybox"
          )))
        ),
        cpus = 0.2,
        mem = 256.0,
        instances = 1
      )
    )

    When("The app is deployed")
    val result = marathon.createAppV2(app)

    Then("The app is created")
    result.code should be (201) // Created
    extractDeploymentIds(result) should have size 1
    waitForEvent("deployment_success")
    waitForTasks(app.id, 1) // The app has really started
  }

  test("create a simple docker app with http health checks") {
    Given("a new app")
    val app = v2AppProxy(testBasePath / "docker-http-app", "v1", instances = 1, withHealth = false, docker = true).
      copy(healthChecks = Set(healthCheck))
    val check = appProxyCheck(app.id, "v1", true)

    When("The app is deployed")
    val result = marathon.createAppV2(app)

    Then("The app is created")
    result.code should be (201) //Created
    extractDeploymentIds(result) should have size 1
    waitForEvent("deployment_success")
    check.pingSince(5.seconds) should be (true) // make sure, the app has really started
  }

  def healthCheck = HealthCheck(gracePeriod = 20.second, interval = 1.second, maxConsecutiveFailures = 10)

  def extractDeploymentIds(app: RestResult[V2AppDefinition]): Seq[String] = {
    try {
      for (deployment <- (app.entityJson \ "deployments").as[JsArray].value)
        yield (deployment \ "id").as[String]
    }
    catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"while parsing:\n${app.entityPrettyJsonString}", e)
    }
  }
}
