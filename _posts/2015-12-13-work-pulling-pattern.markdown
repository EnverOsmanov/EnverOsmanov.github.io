---
layout: post
title: Work pulling pattern
header-img: img/old-bridge-with-green-field-in-front-of-it.jpg
abstract: Пока Reactive Stream от Akka еще не вышел - этот паттерн может вас выручить.
tags:
- scala
- akka
---

Представьте, что у вас есть большой объем данных:

{% highlight scala %}
import scala.io.{BufferedSource, Source}

val bigData: BufferedSource = Source.fromFile("bigData.csv")
{% endhighlight %}

 
И вот вы захотели его как-нибудь обработать - и эта работа долгая и друг от друга не зависимая. А значит, что? Работу нужно распараллелить. Можно, например, каждый кусок работы обернуть в Future:

{% highlight scala %}
  import scala.concurrent.Future
  import scala.concurrent.duration._

  def lineProcessor(line: String): Unit = Thread.sleep(1.minute.toMillis)

  bigData.getLines().foreach{ line =>
    Future(lineProcessor(line))
  }
{% endhighlight %}

Но довольно скоро у вас рухнет приложение из-за OutOfMemory. Все эти данные держать в оперативке и обрабатывать - никакой памяти не напасёшься.
Хмм, ну ок, тогда можно создать роутер с акторами и кидать работу им: 

{% highlight scala %}
  import Messages.Work
  
  val WORKERS_COUNT = 10
  val system = ActorSystem("Akka_system_default")
  val router = system.actorOf(BalancingPool(WORKERS_COUNT).props(Props[Worker]), "router")

  bigData.getLines().foreach { line =>
    router ! Work(line)
  }

  class Worker extends Actor {

    override def receive: Receive = {
      case Work(line) => lineProcessor(line)
    }

  }

  object Messages {
    case class Work(workPart: String)
  }
{% endhighlight %}

Количество одновременно выполняемой работы не будет превышать количества рабочих-акторов в роутере. Все новые сообщения, отправленные роутеру, будут ждать своего звездного часа в его почтовом ящике. Ну, пока этот почтовый ящик не забьётся естественно. И все последующие сообщения вы потеряете, что не есть гуд. 
И вот поэтому нужно сделать так, чтобы рабочие-акторы по окончании своей миссии сами ходили на "почту" за новой работой:

{% highlight scala %}
  import akka.actor.PoisonPill
  import akka.actor.Terminated
  import akka.routing.Broadcast

  import Messages.StartWork
  import Messages.GiveMeWork
  import Messages.WorkAvailable

  class Master extends Actor {

    override def receive: Actor.Receive = waiting


    def waiting: Receive = {
      case StartWork =>
        val curiosRouter = context.actorOf(BalancingPool(WORKERS_COUNT).props(Props[CuriosWorker]), "router")
        context.become(active(bigData.getLines()))
        context.watch(curiosRouter)
        curiosRouter ! Broadcast(WorkAvailable)
    }

    def active(it: Iterator[String]): Receive = {

      case GiveMeWork if it.hasNext => sender ! Work(it.next())

      //if no more work available
      
      case GiveMeWork => sender ! PoisonPill

      //workers completed their last work parts
      
      case Terminated(_) => context.stop(self)

    }

  }

  class CuriosWorker extends Actor {

    override def receive: Receive = {
      case Work(line) =>
        lineProcessor(line)
        sender ! GiveMeWork
    }

  }
{% endhighlight %}

