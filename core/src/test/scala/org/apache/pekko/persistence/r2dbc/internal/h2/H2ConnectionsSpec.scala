/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pekko.persistence.r2dbc.internal.h2

import scala.concurrent.duration._

import org.apache.pekko
import pekko.actor.testkit.typed.scaladsl.LogCapturing
import pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import pekko.persistence.r2dbc.ConnectionFactoryProvider
import pekko.persistence.r2dbc.JournalSettings
import pekko.persistence.r2dbc.QuerySettings
import pekko.persistence.r2dbc.SnapshotSettings
import pekko.persistence.r2dbc.StateSettings
import pekko.persistence.r2dbc.TestConfig
import pekko.persistence.r2dbc.internal.InstantFactory
import pekko.persistence.r2dbc.internal.R2dbcExecutor
import pekko.persistence.r2dbc.journal.JournalDao
import pekko.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import pekko.persistence.r2dbc.query.scaladsl.QueryDao
import pekko.persistence.r2dbc.snapshot.SnapshotDao
import pekko.persistence.r2dbc.state.scaladsl.DurableStateDao
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

class H2ConnectionsSpec extends ScalaTestWithActorTestKit(TestConfig.config) with AnyWordSpecLike with LogCapturing {

  // an H2 connection factory at `path`, independent of the dialect that the tests are run with
  private def h2Config(path: String, h2Overrides: String): Config =
    ConfigFactory.load(
      ConfigFactory
        .parseString(s"""
          $path = $${pekko.persistence.r2dbc.connection-factory}
          $path {
            driver = "h2"
            h2 {
              database = "$path"
              $h2Overrides
            }
          }
          """)
        .withFallback(TestConfig.unresolvedConfig))

  // plugin settings at the root, using the H2 connection factory at `path`
  private def pluginConfig(path: String, pluginPath: String, pluginOverrides: String, h2Overrides: String): Config = {
    val fullConfig = h2Config(path, h2Overrides)
    ConfigFactory
      .parseString(s"""
        dialect = "h2"
        use-connection-factory = "$path"
        use-app-timestamp = on
        db-timestamp-monotonic-increasing = on
        $pluginOverrides
        """)
      .withFallback(fullConfig.getConfig(pluginPath))
      .withFallback(fullConfig)
  }

  private def executor(path: String, config: Config): R2dbcExecutor =
    new R2dbcExecutor(
      ConnectionFactoryProvider(system).connectionFactoryFor(path, config),
      LoggerFactory.getLogger(getClass),
      logDbCallsExceeding = 1.second,
      closeCallsExceeding = None)(system.executionContext, system)

  "H2Connections" should {

    "disable OPTIMIZE_REUSE_RESULTS" in {
      // workaround for a race condition in older H2 versions, users choose the H2 version
      val config = h2Config("h2-reuse-results", "").getConfig("h2-reuse-results.h2")
      H2Connections.configuration(config).getConnectionInfo.getProperty("OPTIMIZE_REUSE_RESULTS", null) shouldBe "FALSE"
    }

    "pass additional-options through to H2" in {
      val path = "h2-additional-options"
      val config = h2Config(path, """additional-options { "DEFAULT_NULL_ORDERING" = "HIGH" }""")
      executor(path, config)
        .selectOne("select setting")(
          _.createStatement(
            "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME = 'DEFAULT_NULL_ORDERING'"),
          _.get(0, classOf[String]))
        .futureValue shouldBe Some("HIGH")
    }

    "create the tables with the configured schema and table names" in {
      val path = "h2-custom-tables"
      val config = h2Config(
        path,
        """
          schema = "custom_schema"
          journal-table = "custom_journal"
          snapshot-table = "custom_snapshot"
          state-table = "custom_state"
          """)
      executor(path, config)
        .select("select tables")(
          _.createStatement(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'CUSTOM_SCHEMA' ORDER BY TABLE_NAME"),
          _.get(0, classOf[String]))
        .futureValue shouldBe Vector("CUSTOM_JOURNAL", "CUSTOM_SNAPSHOT", "CUSTOM_STATE")
    }

    "reject an unknown protocol" in {
      val config = h2Config("h2-unknown-protocol", """protocol = "tcp"""").getConfig("h2-unknown-protocol.h2")
      intercept[IllegalArgumentException] {
        H2Connections.configuration(config)
      }.getMessage should include("Unknown H2 protocol [tcp]")
    }

    "require use-app-timestamp" in {
      val path = "h2-app-timestamp"
      val config = ConfigFactory
        .parseString(s"""
          dialect = "h2"
          use-connection-factory = "$path"
          use-app-timestamp = off
          db-timestamp-monotonic-increasing = on
          """)
        .withFallback(h2Config(path, ""))
        .withFallback(h2Config(path, "").getConfig("pekko.persistence.r2dbc.journal"))
      intercept[IllegalArgumentException] {
        JournalDao.fromConfig(JournalSettings(config), config)(system, system.executionContext)
      }.getMessage should include("use-app-timestamp config must be on for H2 support")
    }

    // the tables are created from the h2 settings of the connection factory, not from the plugin settings
    "reject a journal table that the connection factory doesn't create" in {
      val path = "h2-other-journal-table"
      val config = pluginConfig(path, "pekko.persistence.r2dbc.journal", """table = "other_journal"""", "")
      intercept[IllegalArgumentException] {
        JournalDao.fromConfig(JournalSettings(config), config)(system, system.executionContext)
      }.getMessage should include(s"[$path.h2.journal-table]")
    }

    "reject a query journal table that the connection factory doesn't create" in {
      val path = "h2-other-query-table"
      val config = pluginConfig(path, "pekko.persistence.r2dbc.query", """table = "other_journal"""", "")
      intercept[IllegalArgumentException] {
        QueryDao.fromConfig(QuerySettings(config), config)(system, system.executionContext)
      }.getMessage should include(s"[$path.h2.journal-table]")
    }

    "reject a snapshot table that the connection factory doesn't create" in {
      val path = "h2-other-snapshot-table"
      val config = pluginConfig(path, "pekko.persistence.r2dbc.snapshot", """table = "other_snapshot"""", "")
      intercept[IllegalArgumentException] {
        SnapshotDao.fromConfig(SnapshotSettings(config), config)(system, system.executionContext)
      }.getMessage should include(s"[$path.h2.snapshot-table]")
    }

    "reject a durable state schema that the connection factory doesn't create" in {
      val path = "h2-other-state-schema"
      val config = pluginConfig(path, "pekko.persistence.r2dbc.state", """schema = "other_schema"""", "")
      intercept[IllegalArgumentException] {
        DurableStateDao.fromConfig(StateSettings(config), config)(system, system.executionContext)
      }.getMessage should include(s"[$path.h2.schema]")
    }

    "write to a journal table that the connection factory is configured to create" in {
      val path = "h2-matching-journal-table"
      val config = pluginConfig(
        path,
        "pekko.persistence.r2dbc.journal",
        """table = "other_journal"""",
        """journal-table = "other_journal"""")
      val dao = JournalDao.fromConfig(JournalSettings(config), config)(system, system.executionContext)
      val now = InstantFactory.now()
      val row =
        SerializedJournalRow(1, "Entity", "Entity|p-1", 1L, now, now, Some(Array[Byte](1)), 1, "", "w", Set.empty, None)
      dao.writeEvents(Seq(row)).futureValue
      executor(path, config)
        .selectOne("count")(
          _.createStatement("SELECT count(*) FROM other_journal"),
          _.get(0, classOf[java.lang.Long]).longValue())
        .futureValue shouldBe Some(1L)
    }
  }
}
