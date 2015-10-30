package com.fiser.akka.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri, _}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import play.api.libs.json.Json
import play.modules.reactivemongo.json.BSONFormats
import reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps

object Boot extends App {

  // the actor system to use. Required for flowmaterializer and HTTP.
  // passed in implicit
  implicit val system = ActorSystem("Streams")
  implicit val materializer = ActorMaterializer()

  val serverBinding1: Source[Http.IncomingConnection, Future[Http.ServerBinding]] = Http().bind(interface = "localhost", port = 8091)
  serverBinding1.runForeach { connection =>
    connection.handleWithAsyncHandler(asyncHandler)
  }

  val serverBinding0: Source[Http.IncomingConnection, Future[Http.ServerBinding]] = Http().bind(interface = "localhost", port = 8090)

  val step1: Flow[HttpRequest, String, Unit] = Flow[HttpRequest].mapAsync[String](2)(getTickerHandler("GOOG"))
  val step2: Flow[HttpRequest, String, Unit] = Flow[HttpRequest].mapAsync[String](2)(getTickerHandler("AAPL"))
  val step3: Flow[HttpRequest, String, Unit] = Flow[HttpRequest].mapAsync[String](2)(getTickerHandler("MSFT"))
  val mapToResponse: Flow[String, HttpResponse, Unit] = Flow[String].map[HttpResponse](
    (inp: String) => HttpResponse(status = StatusCodes.OK, entity = inp)
  )

  val broadCastZipFlow = Flow[HttpRequest, HttpResponse]() {
    implicit builder =>
      import FlowGraph.Implicits._

            val broadcast = builder.add(Broadcast[HttpRequest](3))

            val zipWith = ZipWith[String, String, String, HttpResponse] (
              (inp1, inp2, inp3) => new HttpResponse(status = StatusCodes.OK, entity = inp1 + inp2 + inp3))
            val zip = builder.add(zipWith)

            broadcast ~> step1 ~> zip.in0
            broadcast ~> step2 ~> zip.in1
            broadcast ~> step3 ~> zip.in2

      (broadcast.in, zip.out)
  }

  val serverBinding3: Source[Http.IncomingConnection, Future[Http.ServerBinding]] = Http().bind(interface = "localhost", port = 8093)
  serverBinding3.runForeach { connection =>
    connection.handleWith(broadCastZipFlow)
  }

  val broadCastMergeFlow: Flow[HttpRequest, String, Unit] = Flow() {
    implicit builder =>
      import FlowGraph.Implicits._

      val broadcast = builder.add(Broadcast[HttpRequest](3))
      val merge = builder.add(Merge[String](3))

      broadcast ~> step1 ~> merge
      broadcast ~> step2 ~> merge
      broadcast ~> step3 ~> merge
      (broadcast.in, merge.out)
  }
  serverBinding0.runForeach { connection =>
    connection.handleWith(broadCastMergeFlow.via(mapToResponse))
  }

  def getTickerHandler(tickName: String)(request: HttpRequest): Future[String] = {
    val ticker = Database.findTicker(tickName)
    for {
      t <- ticker
    } yield {
      t match {
        case Some(bson) => convertToString(bson)
        case None => ""
      }
    }
  }

  // With an async handler, we use futures. Threads aren't blocked.
  def asyncHandler(request: HttpRequest): Future[HttpResponse] = {
    request match {
      case HttpRequest(HttpMethods.GET, Uri.Path("/getAllTickers"), _, _, _) =>
        for {
          input <- Database.findAllTickers()
        } yield {
          HttpResponse(entity = convertToString(input))
        }
      case HttpRequest(HttpMethods.GET, Uri.Path("/get"), _, _, _) =>
        request.uri.query.get("ticker") match {
          case Some(queryParameter) =>
            val ticker = Database.findTicker(queryParameter)
            for {
              t <- ticker
            } yield {
              t match {
                case Some(bson) => HttpResponse(entity = convertToString(bson))
                case None => HttpResponse(status = StatusCodes.OK)
              }
            }
          case None => Future(HttpResponse(status = StatusCodes.OK))
        }

      case HttpRequest(_, _, _, _, _) =>
        Future[HttpResponse] {
          HttpResponse(status = StatusCodes.NotFound)
        }
    }
  }

  def convertToString(input: List[BSONDocument]): String = {
    input
      .map(f => convertToString(f))
      .mkString("[", ",", "]")
  }

  def convertToString(input: BSONDocument): String = {
    Json.stringify(BSONFormats.toJSON(input))
  }
}