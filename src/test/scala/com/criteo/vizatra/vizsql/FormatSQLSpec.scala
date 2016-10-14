package com.criteo.vizatra.vizsql

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, EitherValues, PropSpec}

class FormatSQLSpec extends PropSpec with Matchers with EitherValues {

  val examples = TableDrivenPropertyChecks.Table(
    ("Input SQL", "Formatted SQL"),

    (
      """SELECT 1""",
      """
        |SELECT
        |  1
      """.stripMargin
    ),

    (
      """select district, sum(population) from city""",
      """
        |SELECT
        |  district,
        |  SUM(population)
        |FROM
        |  city
      """.stripMargin
    ),

    (
      """select * from City as v join Country as p on v.country_id = p.country_id where city.name like ? AND population > 10000""",
      """
        |SELECT
        |  *
        |FROM
        |  city AS v
        |  JOIN country AS p
        |    ON v.country_id = p.country_id
        |WHERE
        |  city.name LIKE ?
        |  AND population > 10000
      """.stripMargin
    ),

    (
      """
        SELECT
          d.device_name as device_name,
          z.description as zone_name,
          z.technology as technology,
          z.affiliate_name as affiliate_name,
          t.country_name as affiliate_country,
          t.country_level_1_name as affiliate_region,
          z.network_name as network_name,
          f.time_id as hour,
          CAST(date_trunc('day', f.time_id) as DATE) as day,
          CAST(date_trunc('week', f.time_id) as DATE) as week,
          CAST(date_trunc('month', f.time_id) as DATE) as month,
          CAST(date_trunc('quarter', f.time_id) as DATE) as quarter,

          SUM(displays) as displays,
          SUM(clicks) as clicks,
          SUM(sales) as sales,
          SUM(order_value_euro * r.rate) as order_value,
          SUM(revenue_euro * r.rate) as revenue,
          SUM(tac_euro * r.rate) as tac,
          SUM((revenue_euro - tac_euro) * r.rate) as revenue_ex_tac,
          SUM(marketplace_revenue_euro * r.rate) as marketplace_revenue,
          SUM((marketplace_revenue_euro - tac_euro) * r.rate) as marketplace_revenue_ex_tac,
          ZEROIFNULL(SUM(clicks)/NULLIF(SUM(displays), 0.0)) as ctr,
          ZEROIFNULL(SUM(sales)/NULLIF(SUM(clicks), 0.0)) as cr,
          ZEROIFNULL(SUM(revenue_euro - tac_euro)/NULLIF(SUM(revenue_euro), 0.0)) as margin,
          ZEROIFNULL(SUM(marketplace_revenue_euro - tac_euro)/NULLIF(SUM(marketplace_revenue_euro), 0.0)) as marketplace_margin,
          ZEROIFNULL(SUM(revenue_euro * r.rate)/NULLIF(SUM(clicks), 0.0)) as cpc,
          ZEROIFNULL(SUM(tac_euro * r.rate)/NULLIF(SUM(displays), 0.0)) * 1000 as cpm
        FROM
          wopr.fact_zone_device_stats_hourly f
          JOIN wopr.dim_zone z
              ON z.zone_id = f.zone_id
          JOIN wopr.dim_device d
              ON d.device_id = f.device_id
          JOIN wopr.dim_country t
              ON t.country_id = f.affiliate_country_id
          JOIN wopr.fact_euro_rates_hourly r
              ON r.currency_id = ?currency_id AND f.time_id = r.time_id

        WHERE
          CAST(f.time_id AS DATE) between ?[day)
          AND t.country_code IN ?{publisher_countries}
          AND d.device_name IN ?{device_name}
          AND z.description IN ?{zone_name}
          AND z.technology IN ?{technology}
          AND z.affiliate_name IN ?{affiliate_name}
          AND t.country_name IN ?{affiliate_country}
          AND t.country_level_1_name IN ?{affiliate_region}
          AND z.network_name IN ?{network_name}

        GROUP BY ROLLUP((
          d.device_name,
          z.description,
          z.technology,
          z.affiliate_name,
          t.country_name,
          z.network_name,
          t.country_level_1_name,
          f.time_id,
          CAST(date_trunc('day', f.time_id) as DATE),
          CAST(date_trunc('week', f.time_id) as DATE),
          CAST(date_trunc('month', f.time_id) as DATE),
          CAST(date_trunc('quarter', f.time_id) as DATE)
        ))
        HAVING SUM(clicks) > 0
      """,
      """
        |SELECT
        |  d.device_name AS device_name,
        |  z.description AS zone_name,
        |  z.technology AS technology,
        |  z.affiliate_name AS affiliate_name,
        |  t.country_name AS affiliate_country,
        |  t.country_level_1_name AS affiliate_region,
        |  z.network_name AS network_name,
        |  f.time_id AS hour,
        |  CAST(DATE_TRUNC('day', f.time_id) AS DATE) AS day,
        |  CAST(DATE_TRUNC('week', f.time_id) AS DATE) AS week,
        |  CAST(DATE_TRUNC('month', f.time_id) AS DATE) AS month,
        |  CAST(DATE_TRUNC('quarter', f.time_id) AS DATE) AS quarter,
        |  SUM(displays) AS displays,
        |  SUM(clicks) AS clicks,
        |  SUM(sales) AS sales,
        |  SUM(order_value_euro * r.rate) AS order_value,
        |  SUM(revenue_euro * r.rate) AS revenue,
        |  SUM(tac_euro * r.rate) AS tac,
        |  SUM((revenue_euro - tac_euro) * r.rate) AS revenue_ex_tac,
        |  SUM(marketplace_revenue_euro * r.rate) AS marketplace_revenue,
        |  SUM((marketplace_revenue_euro - tac_euro) * r.rate) AS marketplace_revenue_ex_tac,
        |  ZEROIFNULL(SUM(clicks) / NULLIF(SUM(displays), 0.0)) AS ctr,
        |  ZEROIFNULL(SUM(sales) / NULLIF(SUM(clicks), 0.0)) AS cr,
        |  ZEROIFNULL(SUM(revenue_euro - tac_euro) / NULLIF(SUM(revenue_euro), 0.0)) AS margin,
        |  ZEROIFNULL(SUM(marketplace_revenue_euro - tac_euro) / NULLIF(SUM(marketplace_revenue_euro), 0.0)) AS marketplace_margin,
        |  ZEROIFNULL(SUM(revenue_euro * r.rate) / NULLIF(SUM(clicks), 0.0)) AS cpc,
        |  ZEROIFNULL(SUM(tac_euro * r.rate) / NULLIF(SUM(displays), 0.0)) * 1000 AS cpm
        |FROM
        |  wopr.fact_zone_device_stats_hourly AS f
        |  JOIN wopr.dim_zone AS z
        |    ON z.zone_id = f.zone_id
        |  JOIN wopr.dim_device AS d
        |    ON d.device_id = f.device_id
        |  JOIN wopr.dim_country AS t
        |    ON t.country_id = f.affiliate_country_id
        |  JOIN wopr.fact_euro_rates_hourly AS r
        |    ON r.currency_id = ?currency_id
        |    AND f.time_id = r.time_id
        |WHERE
        |  CAST(f.time_id AS DATE) BETWEEN ?day
        |  AND t.country_code IN ?publisher_countries
        |  AND d.device_name IN ?device_name
        |  AND z.description IN ?zone_name
        |  AND z.technology IN ?technology
        |  AND z.affiliate_name IN ?affiliate_name
        |  AND t.country_name IN ?affiliate_country
        |  AND t.country_level_1_name IN ?affiliate_region
        |  AND z.network_name IN ?network_name
        |GROUP BY
        |  ROLLUP(
        |    (
        |      d.device_name,
        |      z.description,
        |      z.technology,
        |      z.affiliate_name,
        |      t.country_name,
        |      z.network_name,
        |      t.country_level_1_name,
        |      f.time_id,
        |      CAST(DATE_TRUNC('day', f.time_id) AS DATE),
        |      CAST(DATE_TRUNC('week', f.time_id) AS DATE),
        |      CAST(DATE_TRUNC('month', f.time_id) AS DATE),
        |      CAST(DATE_TRUNC('quarter', f.time_id) AS DATE)
        |    )
        |  )
        |HAVING
        |  SUM(clicks) > 0
      """.stripMargin
    ),

    (
      """select case when a =b then 1 when b <> 2 then 2 else 0 end""",
      """
        |SELECT
        |  CASE
        |    WHEN a = b THEN 1
        |    WHEN b <> 2 THEN 2
        |    ELSE 0
        |  END
      """.stripMargin
    ),

    (
      """select 1,2 union all select 3,4 union all select 5,6""",
      """
        |SELECT
        |  1,
        |  2
        |UNION ALL
        |SELECT
        |  3,
        |  4
        |UNION ALL
        |SELECT
        |  5,
        |  6
      """.stripMargin
    ),

    (
      """select count(distinct woot)""",
      """
        |SELECT
        |  COUNT(DISTINCT woot)
      """.stripMargin
    ),

    (
      """SELECT 1 LIMIT 10""",
      """
        |SELECT
        |  1
        |LIMIT 10
      """.stripMargin
      )
  )

  // --

  property("print SQL") {
    TableDrivenPropertyChecks.forAll(examples) {
      case (sql, expectedSQL) =>
        (new SQL99Parser).parseStatement(sql)
          .fold(e => sys.error(s"\n\n${e.toString(sql)}\n"), identity).toSQL should be (expectedSQL.stripMargin.trim)
    }
  }

}