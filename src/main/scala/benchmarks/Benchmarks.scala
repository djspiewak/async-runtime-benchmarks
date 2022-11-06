package benchmarks

import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 3)
@Warmup(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 3)
@Fork(value = 3, jvmArgsAppend = Array("-Dcats.effect.tracing.mode=none", "-Dcats.effect.auto.yield.threshold.multiplier=20"))  // zio uses a variable threshold, but roughly 10,240
@Threads(value = 1)
class Benchmarks {

  val catsEffectRuntime = cats.effect.unsafe.implicits.global
  val zioRuntime = zio.Runtime.default

  @Benchmark
  def catsEffect3RuntimeChainedFork(): Int = {
    import cats.effect.{Deferred, IO}
    import cats.effect.unsafe.IORuntime

    def catsChainedFork(): Int = {

      def iterate(deferred: Deferred[IO, Unit], n: Int): IO[Any] =
        if (n <= 0) deferred.complete(())
        else IO.unit.flatMap(_ => iterate(deferred, n - 1).start)

      val io = for {
        deferred <- IO.deferred[Unit]
        _ <- iterate(deferred, 1000).start
        _ <- deferred.get
      } yield 0

      runCatsEffect3(io)
    }

    catsChainedFork()
  }

  @Benchmark
  def catsEffect3RuntimeForkMany(): Int = {
    import cats.effect.IO

    def catsEffectForkMany(): Int = {

      def catsEffectRepeat[A](n: Int)(io: IO[A]): IO[A] =
        if (n <= 1) io
        else io.flatMap(_ => catsEffectRepeat(n - 1)(io))

      val io = for {
        deferred <- IO.deferred[Unit]
        ref <- IO.ref(10000)
        effect = ref
          .modify(n => (n - 1, if (n == 1) deferred.complete(()) else IO.unit))
          .flatten
        _ <- catsEffectRepeat(10000)(effect.start)
        _ <- deferred.get
      } yield 0

      runCatsEffect3(io)
    }

    catsEffectForkMany()
  }

  @Benchmark
  def catsEffect3RuntimePingPong(): Int = {
    import cats.effect.{Deferred, IO}
    import cats.effect.std.Queue

    def catsEffectPingPong(): Int = {

      def catsEffectRepeat[A](n: Int)(io: IO[A]): IO[A] =
        if (n <= 1) io
        else io.flatMap(_ => catsEffectRepeat(n - 1)(io))

      def iterate(deferred: Deferred[IO, Unit], n: Int): IO[Any] =
        for {
          ref <- IO.ref(n)
          queue <- Queue.bounded[IO, Unit](1)
          effect = queue.offer(()).start >>
            queue.take >>
            ref
              .modify(n =>
                (n - 1, if (n == 1) deferred.complete(()) else IO.unit)
              )
              .flatten
          _ <- catsEffectRepeat(1000)(effect.start)
        } yield ()

      val io = for {
        deferred <- IO.deferred[Unit]
        _ <- iterate(deferred, 1000).start
        _ <- deferred.get
      } yield 0

      runCatsEffect3(io)
    }

    catsEffectPingPong()
  }

  @Benchmark
  def catsEffect3EnqueueDequeue(): Unit = {
    import cats.effect.IO
    import cats.effect.std.Queue

    def loop(q: Queue[IO, Unit], i: Int): IO[Unit] =
      if (i >= 10000)
        IO.unit
      else
        q.offer(()).flatMap(_ => q.take.flatMap(_ => loop(q, i + 1)))

    runCatsEffect3(Queue.bounded[IO, Unit](1).flatMap(loop(_, 0)))
  }

  @Benchmark
  def catsEffect3RuntimeYieldMany(): Int = {
    import cats.effect.IO

    def catsEffectYieldMany(): Int = {

      def catsEffectRepeat[A](n: Int)(io: IO[A]): IO[A] =
        if (n <= 1) io
        else io.flatMap(_ => catsEffectRepeat(n - 1)(io))

      val io = for {
        deferred <- IO.deferred[Unit]
        ref <- IO.ref(200)
        effect =
          catsEffectRepeat(1000)(IO.cede) >> ref
            .modify(n =>
              (n - 1, if (n == 1) deferred.complete(()) else IO.unit)
            )
            .flatten
        _ <- catsEffectRepeat(200)(effect.start)
        _ <- deferred.get
      } yield 0

      runCatsEffect3(io)
    }

    catsEffectYieldMany()
  }

  @Benchmark
  def catsEffect3DeepBind(): Unit = {
    import cats.effect.IO

    def loop(i: Int): IO[Unit] =
      IO.unit.flatMap { _ =>
        if (i > 10000)
          IO.unit
        else
          loop(i + 1)
      }

    runCatsEffect3(loop(0))
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MINUTES)
  def catsEffect3Scheduling(): Int = {
    import cats.effect.IO
    import cats.syntax.traverse._

    def schedulingBenchmark(): Int = {
      def fiber(i: Int): IO[Int] =
        IO.cede.flatMap { _ =>
          IO(i).flatMap { j =>
            IO.cede.flatMap { _ =>
              if (j > 10000)
                IO.cede.flatMap(_ => IO(j))
              else
                IO.cede.flatMap(_ => fiber(j + 1))
            }
          }
        }

      val io = List
        .range(0, 1000000)
        .traverse(fiber(_).start)
        .flatMap(_.traverse(_.joinWithNever))
        .map(_.sum)

      runCatsEffect3(io)
    }

    schedulingBenchmark()
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MINUTES)
  def catsEffect3Alloc(): Int = {
    import cats.effect.IO
    import cats.syntax.traverse._

    def alloc(): Int = {
      def allocation(n: Int): IO[Array[AnyRef]] =
        IO {
          val size = math.max(100, math.min(n, 2000))
          val array = new Array[AnyRef](size)
          for (i <- (0 until size)) {
            array(i) = new AnyRef()
          }
          array
        }

      def sum(array: Array[AnyRef]): IO[Int] =
        IO {
          array.map(_.hashCode()).sum
        }

      def fiber(i: Int): IO[Int] =
        IO.cede.flatMap { _ =>
          allocation(i).flatMap { arr =>
            IO.cede.flatMap(_ => sum(arr)).flatMap { _ =>
              if (i > 1000)
                IO.cede.flatMap(_ => IO(i))
              else
                IO.cede.flatMap(_ => fiber(i + 1))
            }
          }
        }

      val io = List
        .range(0, 2500)
        .traverse(_ => fiber(0).start)
        .flatMap(_.traverse(_.joinWithNever))
        .map(_.sum)

      runCatsEffect3(io)
    }

    alloc()
  }

  @Benchmark
  def zio2SchedulerChainedFork(): Int = {
    import zio.{Promise, ZIO}

    def zioChainedFork(): Int = {

      def iterate(promise: Promise[Nothing, Unit], n: Int): ZIO[Any, Nothing, Any] =
        if (n <= 0) promise.succeed(())
        else ZIO.unit.flatMap(_ => iterate(promise, n - 1).forkDaemon)

      val io = for {
        promise <- Promise.make[Nothing, Unit]
        _ <- iterate(promise, 1000).forkDaemon
        _ <- promise.await
      } yield 0

      runZIO(io)
    }

    zioChainedFork()
  }

  @Benchmark
  def zio2SchedulerForkMany(): Int = {
    import zio.{Promise, Ref, ZIO}

    def zioForkMany(): Int = {

      def repeat[R, E, A](n: Int)(zio: ZIO[R, E, A]): ZIO[R, E, A] =
        if (n <= 1) zio
        else zio *> repeat(n - 1)(zio)

      val io = for {
        promise <- Promise.make[Nothing, Unit]
        ref <- Ref.make(10000)
        effect = ref
          .modify(n => (if (n == 1) promise.succeed(()) else ZIO.unit, n - 1))
          .flatten
        _ <- repeat(10000)(effect.forkDaemon)
        _ <- promise.await
      } yield 0

      runZIO(io)
    }

    zioForkMany()
  }

  @Benchmark
  def zio2SchedulerPingPong(): Int = {
    import zio.{Promise, Queue, Ref, ZIO}

    def zioPingPong(): Int = {

      def repeat[R, E, A](n: Int)(zio: ZIO[R, E, A]): ZIO[R, E, A] =
        if (n <= 1) zio
        else zio *> repeat(n - 1)(zio)

      def iterate(promise: Promise[Nothing, Unit], n: Int): ZIO[Any, Nothing, Any] =
        for {
          ref <- Ref.make(n)
          queue <- Queue.bounded[Unit](1)
          effect = queue.offer(()).forkDaemon *>
            queue.take *>
            ref
              .modify(n =>
                (if (n == 1) promise.succeed(()) else ZIO.unit, n - 1)
              )
              .flatten
          _ <- repeat(1000)(effect.forkDaemon)
        } yield ()

      val io = for {
        promise <- Promise.make[Nothing, Unit]
        _ <- iterate(promise, 1000).forkDaemon
        _ <- promise.await
      } yield 0

      runZIO(io)
    }

    zioPingPong()
  }

  @Benchmark
  def zio2EnqueueDequeue(): Unit = {
    import zio.{ZIO, Queue}

    def loop(q: Queue[Unit], i: Int): ZIO[Any, Nothing, Unit] =
      if (i >= 10000)
        ZIO.unit
      else
        q.offer(()).flatMap(_ => q.take.flatMap(_ => loop(q, i + 1)))

    runZIO(Queue.bounded[Unit](1).flatMap(loop(_, 0)))
  }

  @Benchmark
  def zio2SchedulerYieldMany(): Int = {
    import zio.{Promise, Ref, ZIO}

    def zioYieldMany(): Int = {

      def repeat[R, E, A](n: Int)(zio: ZIO[R, E, A]): ZIO[R, E, A] =
        if (n <= 1) zio
        else zio *> repeat(n - 1)(zio)

      val io = for {
        promise <- Promise.make[Nothing, Unit]
        ref <- Ref.make(200)
        effect =
          repeat(1000)(ZIO.yieldNow) *> ref
            .modify(n => (if (n == 1) promise.succeed(()) else ZIO.unit, n - 1))
            .flatten
        _ <- repeat(200)(effect.forkDaemon)
        _ <- promise.await
      } yield 0

      runZIO(io)
    }

    zioYieldMany()
  }

  @Benchmark
  def zio2DeepBind(): Unit = {
    import zio.ZIO

    def loop(i: Int): ZIO[Any, Nothing, Unit] =
      ZIO.unit.flatMap { _ =>
        if (i > 10000)
          ZIO.unit
        else
          loop(i + 1)
      }

    runZIO(loop(0))
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MINUTES)
  def zio2Scheduling(): Int = {
    import zio.ZIO

    def schedulingBenchmark(): Int = {
      def fiber(i: Int): ZIO[Any, Throwable, Int] =
        ZIO.yieldNow.flatMap { _ =>
          ZIO.from(i).flatMap { j =>
            ZIO.yieldNow.flatMap { _ =>
              if (j > 10000)
                ZIO.yieldNow.flatMap(_ => ZIO.from(j))
              else
                ZIO.yieldNow.flatMap(_ => fiber(j + 1))
            }
          }
        }

      val io = ZIO
        .foreach(List.range(0, 1000000))(n => fiber(n).forkDaemon)
        .flatMap { list =>
          ZIO.foreach(list)(_.join)
        }
        .map(_.sum)

      runZIO(io)
    }

    schedulingBenchmark()
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MINUTES)
  def zio2Alloc(): Int = {
    import zio.ZIO

    def alloc(): Int = {
      def allocation(n: Int) =
        ZIO from {
          val size = math.max(100, math.min(n, 2000))
          val array = new Array[AnyRef](size)
          for (i <- (0 until size)) {
            array(i) = new AnyRef()
          }
          array
        }

      def sum(array: Array[AnyRef]) =
        ZIO from {
          array.map(_.hashCode()).sum
        }

      def fiber(i: Int): ZIO[Any, Throwable, Int] =
        ZIO.yieldNow.flatMap { _ =>
          allocation(i).flatMap { arr =>
            ZIO.yieldNow.flatMap(_ => sum(arr)).flatMap { _ =>
              if (i > 1000)
                ZIO.yieldNow.flatMap(_ => ZIO.from(i))
              else
                ZIO.yieldNow.flatMap(_ => fiber(i + 1))
            }
          }
        }

      val io = ZIO
        .foreach(List.range(0, 2500))(_ => fiber(0).forkDaemon)
        .flatMap { list =>
          ZIO.foreach(list)(_.join)
        }
        .map(_.sum)

      runZIO(io)
    }

    alloc()
  }

  // we insert leading yields for both runtimes to remove the "synchronous prefix" optimization

  private[this] def runCatsEffect3[A](io: cats.effect.IO[A]): A =
    (cats.effect.IO.cede.flatMap(_ => io)).unsafeRunSync()(catsEffectRuntime)

  private[this] def runZIO[A](io: zio.ZIO[Any, Throwable, A]): A =
    zio.Unsafe.unsafe(implicit u => zioRuntime.unsafe.run(zio.ZIO.yieldNow.flatMap(_ => io)).getOrThrow)
}
