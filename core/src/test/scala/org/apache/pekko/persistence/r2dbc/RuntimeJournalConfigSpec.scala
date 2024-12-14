/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.persistence.r2dbc

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.pekko.Done
import org.apache.pekko.actor.testkit.typed.scaladsl.LogCapturing
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.persistence.r2dbc.RuntimeJournalConfigSpec.RuntimeJournal
import org.apache.pekko.persistence.r2dbc.journal.R2dbcJournalSpec
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.Effect
import org.apache.pekko.persistence.typed.scaladsl.Effect
import org.apache.pekko.persistence.typed.scaladsl.Effect
import org.apache.pekko.persistence.typed.scaladsl.EventSourcedBehavior
import org.apache.pekko.persistence.typed.scaladsl.RetentionCriteria
import org.scalatest.Inspectors.forEvery
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object RuntimeJournalConfigSpec {
  val config = R2dbcJournalSpec.config

  sealed trait RuntimeJournal {
    def name: String
    def database: String
    final def config: Config = ConfigFactory.load(
      ConfigFactory.parseString(s"""
        |$name.journal.connection-settings.connection-factory.database = "$database"
        |""".stripMargin)
        .withFallback(RuntimeJournalConfigSpec.config)
        .withFallback(
          ConfigFactory.parseString(s"""
            |$name.journal = $${pekko.persistence.r2dbc.journal}
            |""".stripMargin)
        )
    )
  }
  object RuntimeJournal {
    case object Journal1 extends RuntimeJournal {
      override def name: String = "journal1"
      override def database: String = "runtime_journal_1"
    }
    case object Journal2 extends RuntimeJournal {
      override def name: String = "journal2"
      override def database: String = "runtime_journal_2"
    }
  }

  object Actor {
    sealed trait Command
    case class Save(text: String, replyTo: ActorRef[Done]) extends Command
    case class ShowMeWhatYouGot(replyTo: ActorRef[String]) extends Command
    case object Stop extends Command

    def apply(persistenceId: String, journal: RuntimeJournal): Behavior[Command] =
      EventSourcedBehavior[Command, String, String](
        PersistenceId.ofUniqueId(persistenceId),
        "",
        (state, cmd) =>
          cmd match {
            case Save(text, replyTo) =>
              Effect.persist(text).thenRun(_ => replyTo ! Done)
            case ShowMeWhatYouGot(replyTo) =>
              replyTo ! state
              Effect.none
            case Stop =>
              Effect.stop()
          },
        (state, evt) => Seq(state, evt).filter(_.nonEmpty).mkString("|"))
        .withJournalPluginId(s"${journal.name}.journal")
        .withJournalPluginConfig(Some(journal.config))
//        .withRetention(RetentionCriteria.snapshotEvery(1, Int.MaxValue))
//        .withSnapshotPluginId(s"$journal.snapshot")
//        .withSnapshotPluginConfig(Some(config(journal)))

  }
}

class RuntimeJournalSpec extends AnyWordSpecLike with Matchers with ScalaFutures with LogCapturing {
  import RuntimeJournalConfigSpec._

  "work 1" in {
    forEvery(RuntimeJournal.Journal1 :: RuntimeJournal.Journal2 :: Nil) { journal =>
      val config = journal.config
      val basePath = "pekko.persistence.r2dbc.journal.connection-settings.connection-factory.database"
      config.getString(basePath) shouldEqual "postgres"
      val specificPath = s"${journal.name}.journal.connection-settings.connection-factory.database"
      config.getString(specificPath) shouldEqual journal.database
    }
  }

  "work 2" in {
    val config =
      ConfigFactory
        .parseString("""
    runtime1 {
      journal {
        class = "org.apache.pekko.persistence.r2dbc.journal.R2dbcJournal"
        shared = ${runtime1.shared}

        # etc...
      }

      snapshot {
        class = "org.apache.pekko.persistence.r2dbc.snapshot.R2dbcSnapshotStore"
        shared = ${runtime1.shared}

        # etc...
      }

      shared = {
        dialect = "postgres"

        connection-factory {
          database = "runtime1_database"

          # etc...
        }
      }
    }
  """).resolve()

    config.getString("runtime1.journal.shared.connection-factory.database") shouldBe "runtime1_database"
    config.getString("runtime1.snapshot.shared.connection-factory.database") shouldBe "runtime1_database"
  }

  "work 3" in {
    val config = ConfigFactory
      .parseString("""
    runtime1 {
      journal = ${pekko.persistence.r2dbc.journal}
      journal.shared = ${runtime1.shared}

      snapshot = ${pekko.persistence.r2dbc.snapshot}
      snapshot.shared = ${runtime1.shared}

      shared = ${pekko.persistence.r2dbc.connection-settings}
      shared = {
        dialect = "postgres"

        connection-factory {
          database = "runtime1_database"
        }
      }
    }
  """)
      .withFallback(RuntimeJournalConfigSpec.config)
      .resolve()

    config.getString("runtime1.journal.shared.connection-factory.database") shouldBe "runtime1_database"
    config.getString("runtime1.snapshot.shared.connection-factory.database") shouldBe "runtime1_database"
    config.getInt("runtime1.journal.shared.connection-factory.max-size") shouldBe 20
    config.getInt("runtime1.snapshot.shared.connection-factory.max-size") shouldBe 20
  }
}

class RuntimeJournalConfigSpec extends ScalaTestWithActorTestKit(RuntimeJournalConfigSpec.config) with AnyWordSpecLike
    with ScalaFutures
    with LogCapturing with TestDbLifecycle {
  import RuntimeJournalConfigSpec._

  override implicit lazy val typedSystem: typed.ActorSystem[_] = system

  "work" in {
    val probe = createTestProbe[Any]()

    val j1 = spawn(Actor("pid1", RuntimeJournal.Journal1))
    j1 ! Actor.Save("j1m1", probe.ref)
    probe.receiveMessage()

    ???
  }
}
