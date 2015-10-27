/*
 * Copyright 2003-2015 Monitise Group Limited. All Rights Reserved.
 *
 * Save to the extent permitted by law, you may not use, copy, modify,
 * distribute or create derivative works of this material or any part
 * of it without the prior written consent of Monitise Group Limited.
 * Any reproduction of this material must contain this notice.
 */
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

object Boot extends App {

  // the actor system to use. Required for flowmaterializer and HTTP.
  // passed in implicit
  implicit val system = ActorSystem("Streams")
  implicit val materializer = ActorMaterializer()

  val serverBinding1: Source[Http.IncomingConnection, Future[Http.ServerBinding]] = Http().bind(interface = "localhost", port = 8091)

  serverBinding1.runForeach { connection =>
    connection.handleWithAsyncHandler(asyncHandler)
  }

  val serverBinding2: Source[Http.IncomingConnection, Future[Http.ServerBinding]] = Http().bind(interface = "localhost", port = 8090)

  val step1 = Flow[HttpRequest].mapAsync[String](2)(getTickerHandler("GOOG"))
  val step2 = Flow[HttpRequest].mapAsync[String](2)(getTickerHandler("AAPL"))
  val step3 = Flow[HttpRequest].mapAsync[String](2)(getTickerHandler("MSFT"))


  val mapToResponse: Flow[String, HttpResponse, Unit] = Flow[String].map[HttpResponse](
    (inp: String) => HttpResponse(status = StatusCodes.OK, entity = inp)
  )

  val broadCastMergeFlow = Flow[HttpRequest, HttpResponse, Unit]() { implicit builder =>
    import FlowGraph.Implicits._

    val broadcast = builder.add(Broadcast[HttpRequest](3))
    val merge =  builder.add(Merge[String](3))

           broadcast ~> step1 ~> merge
           broadcast ~> step2 ~> merge ~> mapToResponse ~ Sink.foreach(identity)
           broadcast ~> step3 ~> merge

      (broadcast.in, merge.out)
  }

  serverBinding2.runForeach { connection =>
    connection.handleWithAsyncHandler(broadCastMergeFlow)
  }

  def getTickerHandler(tickName: String)(request: HttpRequest): Future[String] = {
    val ticker = Database.findTicker(tickName)
    Thread.sleep(Math.random() * 1000 toInt)
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