import java.io.File
import java.nio.file.Files
import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.collect.ImmutableMap
import com.spotify.docker.client.DockerClient.LogsParam
import com.spotify.docker.client.messages.{ Container, ContainerConfig, HostConfig, PortBinding }
import com.spotify.docker.client.{ DefaultDockerClient, DockerClient }
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }
import org.slf4j.{ Logger, LoggerFactory }
import reactivemongo.api.Cursor.WithOps
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{ Cursor, MongoConnection, MongoDriver }
import reactivemongo.bson.BSONDocument

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.language.postfixOps
import scala.util.{ Failure, Success }

class CursorRetrySpec extends FlatSpec with Matchers with BeforeAndAfter {

  private val containerName                = "reactivemongo-dev"
  private val image                        = "mongo:3.6"
  private val logger: Logger               = LoggerFactory.getLogger(classOf[CursorRetrySpec])
  private val insertCount                  = new AtomicInteger(0)
  private val receivedCount                = new AtomicInteger(0)
  private var docker: Option[DockerClient] = None
  private val exposedPort                  = 32200 + (new scala.util.Random).nextInt(5000)

  before {

    docker = Some(DefaultDockerClient.fromEnv().build())

    docker.foreach(d => {

      getContainer match {
        case Some(c) if c.state() == "running" =>
          logger.info(s"Container is already running with ID: ${c.id()}")
        case Some(c) =>
          logger.info(s"Starting container with ID: ${c.id()}")
          d.startContainer(c.id())
        case None =>
          logger.info("Creating new container.")
          startNewContainer(d)
      }

    })

  }

  "Cursor" should "handle server closing connection" in {
    val container = getContainer.get

    val driver = new MongoDriver()
    val connection: MongoConnection = driver
      .connection(
        s"mongodb://${docker.get.getHost}:${container.ports().get(0).publicPort()}/db"
      )
      .get
    val collection: Future[BSONCollection] =
      connection.database("db").map(_.collection("collection"))

//    Thread.sleep(10000)
    println("--------------------------------------------------------")

    // Ensure db and collection
    insertDoc(collection)
    insertCount.decrementAndGet()

    val future = watch(collection)
    future.onComplete {
      case Success(_) => logger.info("Watch completed successful")
      case Failure(t) => logger.info(s"Watch completed with failure: ${t.getMessage}")
    }
    Thread.sleep(50)

    insertDoc(collection)

    Thread.sleep(50)

    receivedCount.get() shouldBe insertCount.get()

    restartContainer()

    Thread.sleep(50)

    insertDoc(collection)

    Thread.sleep(50)

    insertCount.get() shouldBe 2

    Thread.sleep(30000)

    future.isCompleted shouldBe true

    receivedCount.get() shouldBe insertCount.get()

    println("--------------------------------------------------------")
    Thread.sleep(10000)

    connection.askClose()(5 seconds)

  }

  private def insertDoc(collection: Future[BSONCollection]) =
    Await.result(
      collection
        .flatMap(
          _.insert
            .one(BSONDocument("firstName" -> "Stephane", "lastName" -> "Godbillon", "age" -> 29))
        )
        .map(_ => insertCount.incrementAndGet()),
      50 seconds
    )

  private def watch(collection: Future[BSONCollection]) =
    collection
      .flatMap { col =>
        val cursor = col
          .watch[BSONDocument](maxAwaitTimeMS = Some(100))
          .cursor[WithOps]
        cursor
          .foldWhile(List.empty[BSONDocument])(
            { (ls, doc) =>
              logger.info(s"Received document: ${BSONDocument.pretty(doc)}")
              receivedCount.incrementAndGet()
              Cursor.Cont(ls)
            }, { (ls, err) => // handle failure
              err match {
                case _ =>
                  logger.error("Watching cursor failed.", err)
                  Cursor.Fail(err) // Stop with current failure -> Future.failed
              }
            }
          )
      }

  private def getContainer: Option[Container] =
    docker.flatMap(
      _.listContainers(DockerClient.ListContainersParam.allContainers()).asScala
        .find(_.names().contains(s"/$containerName"))
    )

  private def startNewContainer(d: DockerClient): Unit = {
    d.pull(image)
    val container = d.createContainer(
      ContainerConfig
        .builder()
        .image(image)
        .hostConfig(
          HostConfig
            .builder()
            .portBindings(
              ImmutableMap
                .of("27017/tcp", util.Arrays.asList(PortBinding.of("", exposedPort)))
            )
            .build()
        )
        .exposedPorts(s"27017/tcp")
        .cmd("mongod", "--replSet", "rs")
        .env("MONGO_INITDB_DATABASE=db")
        .build(),
      containerName
    )
    logger.info(s"Created container with ID: ${container.id()}")

    val tempDir = Files.createTempDirectory("")

    val rsInitFile = new File(tempDir.toFile, "0001_RS_INIT.js")

    Files.write(rsInitFile.toPath, "rs.initiate();".getBytes)

    d.copyToContainer(tempDir, container.id(), "/docker-entrypoint-initdb.d")

    logger.info(s"Starting container with ID: ${container.id()}")
    d.startContainer(container.id())

    val waitFor =
      ".*\\[rsSync\\] transition to primary complete; database writes are now permitted.*".r

    while (true) {

      val str = d.logs(container.id(), LogsParam.stdout(), LogsParam.tail(15)).readFully()
      if (waitFor.findFirstIn(str).isDefined)
        return
    }

  }

  private def restartContainer(): Unit =
    docker.foreach(d => {
      getContainer match {
        case Some(c) =>
          logger.info(s"Restarting Container with ID: ${c.id()}")
          d.restartContainer(c.id(), 10)

          val waitFor =
            ".*\\[rsSync\\] transition to primary complete; database writes are now permitted.*".r

          while (true) {

            val str = d.logs(c.id(), LogsParam.stdout(), LogsParam.tail(15)).readFully()
            if (waitFor.findFirstIn(str).isDefined)
              return
          }
        case _ =>
      }
    })

  after {
    docker.foreach(d => {
      getContainer match {
        case Some(c) =>
          logger.info(s"Stopping Container with ID: ${c.id()}")
          d.stopContainer(c.id(), 10)
          d.removeContainer(c.id())
        case _ =>
      }

      d.close()
    })
    docker = None
  }
}
