package com.criteo.vizatra.vizsql

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, EitherValues, FlatSpec}

class OlapSpec extends FlatSpec with Matchers with EitherValues {

  implicit val VERTICA = new Dialect {

    def parser = new SQL99Parser

    val functions = SQLFunction.standard orElse {
      case "date_trunc" => new SQLFunction2 {
        def result = { case ((_, _), (_, t)) => Right(TIMESTAMP(nullable = t.nullable)) }
      }
      case "zeroifnull" => new SQLFunction1 {
        def result = { case (_, t) => Right(t.withNullable(false)) }
      }
      case "nullifzero" => new SQLFunction1 {
        def result = { case (_, t) => Right(t.withNullable(true)) }
      }
    }: PartialFunction[String,SQLFunction]
  }

  val BIDATA = DB(schemas = List(
    Schema(
      "wopr",
      tables = List(
        Table(
          "fact_zone_device_stats_hourly",
          columns = List(
            Column("time_id", TIMESTAMP(nullable = false)),
            Column("zone_id", INTEGER(nullable = false)),
            Column("device_id", INTEGER(nullable = false)),
            Column("affiliate_country_id", INTEGER(nullable = false)),
            Column("displays", INTEGER(nullable = true)),
            Column("clicks", INTEGER(nullable = true)),
            Column("revenue_euro", DECIMAL(nullable = true)),
            Column("order_value_euro", DECIMAL(nullable = true)),
            Column("sales", DECIMAL(nullable = true)),
            Column("marketplace_revenue_euro", DECIMAL(nullable = true)),
            Column("tac_euro", DECIMAL(nullable = true)),
            Column("network_id", INTEGER(nullable = false))
          )
        ),
        Table(
          "dim_zone",
          columns = List(
            Column("zone_id", INTEGER(nullable = false)),
            Column("description", STRING(nullable = true)),
            Column("network_name", STRING(nullable = true)),
            Column("affiliate_name", STRING(nullable = true)),
            Column("technology", STRING(nullable = true))
          )
        ),
        Table(
          "dim_device",
          columns = List(
            Column("device_id", INTEGER(nullable = false)),
            Column("device_name", STRING(nullable = true))
          )
        ),
        Table(
          "dim_country",
          columns = List(
            Column("country_id", INTEGER(nullable = false)),
            Column("country_code", STRING(nullable = true)),
            Column("country_name", STRING(nullable = true)),
            Column("country_level_1_name", STRING(nullable = true))
          )
        ),
        Table(
          "fact_euro_rates_hourly",
          columns = List(
            Column("time_id", TIMESTAMP(nullable = false)),
            Column("currency_id", INTEGER(nullable = false)),
            Column("rate", DECIMAL(nullable = true))
          )
        ),
        Table(
          "fact_portfolio",
          columns = List(
            Column("time_id", TIMESTAMP(nullable = false)),
            Column("affiliate_id", INTEGER(nullable = false)),
            Column("network_id", INTEGER(nullable = false)),
            Column("zone_id", INTEGER(nullable = false)),
            Column("device_id", INTEGER(nullable = false)),
            Column("affiliate_country_id", INTEGER(nullable = false)),
            Column("affiliate_region_name", STRING(nullable = false)),
            Column("displays", INTEGER(nullable = true)),
            Column("clicks", INTEGER(nullable = true)),
            Column("revenue_euro", DECIMAL(nullable = true)),
            Column("order_value_euro", DECIMAL(nullable = true)),
            Column("sales", DECIMAL(nullable = true)),
            Column("marketplace_revenue_euro", DECIMAL(nullable = true)),
            Column("tac_euro", DECIMAL(nullable = true))
          )
        )
      )
    )
  ))

  val QUERY =
  """
    SELECT
        d.device_name as device_name,
        z.description as zone_name,
        z.technology as technology,
        z.affiliate_name as affiliate_name,
        t.country_name as affiliate_country,
        t.country_level_1_name as affiliate_region,
        z.network_name as network_name,
        date_trunc('hour', f.time_id) as hour,
        date_trunc('day', f.time_id) as day,
        date_trunc('week', f.time_id) as week,
        date_trunc('month', f.time_id) as month,
        date_trunc('quarter', f.time_id) as quarter,

        SUM(displays) as displays,
        SUM(clicks) as clicks,
        SUM(sales) as sales,
        SUM(order_value_euro * r.rate) as order_value,
        SUM(revenue_euro * r.rate) as revenue,
        SUM(tac_euro * r.rate) as tac,
        SUM((revenue_euro - tac_euro) * r.rate) as revenue_ex_tac,
        SUM(marketplace_revenue_euro * r.rate) as marketplace_revenue,
        SUM((marketplace_revenue_euro - tac_euro) * r.rate) as marketplace_revenue_ex_tac,
        ZEROIFNULL(SUM(clicks)/NULLIFZERO(SUM(displays))) as ctr,
        ZEROIFNULL(SUM(sales)/NULLIFZERO(SUM(clicks))) as cr,
        ZEROIFNULL(SUM(revenue_euro - tac_euro)/NULLIFZERO(SUM(revenue_euro))) as margin,
        ZEROIFNULL(SUM(marketplace_revenue_euro - tac_euro)/NULLIFZERO(SUM(marketplace_revenue_euro))) as marketplace_margin,
        ZEROIFNULL(SUM(revenue_euro * r.rate)/NULLIFZERO(SUM(clicks))) as cpc,
        ZEROIFNULL(SUM(tac_euro * r.rate)/NULLIFZERO(SUM(displays))) * 1000 as cpm
    FROM
        wopr.fact_zone_device_stats_hourly f
        JOIN wopr.dim_zone z
            ON z.zone_id = f.zone_id
        JOIN wopr.dim_device d
            ON d.device_id = f.device_id
        JOIN wopr.dim_country t
            ON t.country_name = t2.country_name
        JOIN wopr.fact_euro_rates_hourly r
            ON r.currency_id = ?currency_id AND f.time_id = r.time_id
        JOIN wopr.dim_country t2
          ON t2.country_id = f.affiliate_country_id

    WHERE
        CAST(f.time_id AS DATE) between ?[date_range)
        AND t.country_code IN ?{publisher_countries}
        AND d.device_name IN ?{device_name}
        AND z.description IN ?{zone_name}
        AND z.technology IN ?{technology}
        AND z.affiliate_name IN ?{affiliate_name}
        AND t.country_name IN ?{affiliate_country}
        AND t.country_level_1_name IN ?{affiliate_region}
        AND z.network_name IN ?{network_name}

    GROUP BY ROLLUP(
      (
        d.device_name,
        z.description,
        z.technology,
        z.affiliate_name,
        t.country_name,
        z.network_name,
        t.country_level_1_name,
        date_trunc('hour', f.time_id),
        date_trunc('day', f.time_id),
        date_trunc('week', f.time_id),
        date_trunc('month', f.time_id),
        date_trunc('quarter', f.time_id)
      )
    )

    ORDER BY
      date_trunc('hour', f.time_id),
      date_trunc('day', f.time_id),
      date_trunc('week', f.time_id),
      date_trunc('month', f.time_id),
      date_trunc('quarter', f.time_id)
  """

  // -- Actual tests

  val olapQuery = VizSQL.parseOlapQuery(QUERY, BIDATA).left.map(_.toString(QUERY))

  "An OLAP query" should "recognize time dimensions" in {
    olapQuery.right.flatMap(_.getTimeDimensions) should be (Right(List(
      "hour",
      "day",
      "week",
      "month",
      "quarter"
    )))
  }

  it should "recognize other dimensions" in {
    olapQuery.right.flatMap(_.getDimensions) should be (Right(List(
      "device_name",
      "zone_name",
      "technology",
      "affiliate_name",
      "affiliate_country",
      "affiliate_region",
      "network_name"
    )))
  }

  it should "recognize metrics" in {
    olapQuery.right.flatMap(_.getMetrics) should be (Right(List(
      "displays",
      "clicks",
      "sales",
      "order_value",
      "revenue",
      "tac",
      "revenue_ex_tac",
      "marketplace_revenue",
      "marketplace_revenue_ex_tac",
      "ctr",
      "cr",
      "margin",
      "marketplace_margin",
      "cpc",
      "cpm"
    )))
  }

  it should "recognize input parameters" in {
    olapQuery.right.flatMap(_.getParameters) should be (Right(List(
      "currency_id",
      "date_range",
      "publisher_countries",
      "device_name",
      "zone_name",
      "technology",
      "affiliate_name",
      "affiliate_country",
      "affiliate_region",
      "network_name"
    )))
  }

  it should "retrieve the projection expression for dimensions/metrics" in {
    val examples = TableDrivenPropertyChecks.Table(
      ("Dimension or metric", "Expression"),

      ("revenue", """SUM(revenue_euro * r.rate) AS revenue"""),
      ("affiliate_region", """t.country_level_1_name AS affiliate_region""")
    )

    TableDrivenPropertyChecks.forAll(examples) {
      case (dimensionOrMetric, expression) =>
        olapQuery.right.flatMap(_.getProjection(dimensionOrMetric).right.map(_.toSQL)) should be (Right(
          expression
        ))
    }
  }

  it should "retrieve the tables needed for dimensions/metrics" in {
    val examples = TableDrivenPropertyChecks.Table(
      ("Dimension", "Used tables"),

      ("zone_name", List("z")),
      ("marketplace_margin", List("f")),
      ("cpc", List("f", "r")),
      ("affiliate_region", List("t"))
    )

    TableDrivenPropertyChecks.forAll(examples) {
      case (dim, tables) =>
        olapQuery.right.flatMap(q =>
          q.getProjection(dim).right.map(_.expression).right.flatMap(OlapQuery.tablesFor(q.query.select, q.query.db, _))
        ) should be (Right(tables))
    }
  }

  it should "rewrite FROM relations based on a given table set" in {
    val examples = TableDrivenPropertyChecks.Table(
      ("Tables", "FROM clause"),

      (Set.empty[String], ""),

      (Set("f"),
        """wopr.fact_zone_device_stats_hourly AS f"""
      ),

      (Set("t"),
        """wopr.dim_country AS t"""
      ),

      (Set("f", "t"),
        """
          |wopr.fact_zone_device_stats_hourly AS f
          |JOIN wopr.dim_country AS t
          |  ON t.country_name = t2.country_name
          |JOIN wopr.dim_country AS t2
          |  ON t2.country_id = f.affiliate_country_id
        """
      ),

      (Set("f", "z", "d"),
        """
          |wopr.fact_zone_device_stats_hourly AS f
          |JOIN wopr.dim_zone AS z
          |  ON z.zone_id = f.zone_id
          |JOIN wopr.dim_device AS d
          |  ON d.device_id = f.device_id
        """
      ),

      (Set("f", "r"),
        """
          |wopr.fact_zone_device_stats_hourly AS f
          |JOIN wopr.fact_euro_rates_hourly AS r
          |  ON r.currency_id = ?currency_id
          |  AND f.time_id = r.time_id
        """
      )
    )

    TableDrivenPropertyChecks.forAll(examples) {
      case (tables, expectedSQL) =>
        olapQuery.right.map(q => OlapQuery.rewriteRelations(q.query.select, BIDATA, tables)
          .map(_.toSQL).mkString(",\n")) should be (Right(expectedSQL.stripMargin.trim))
    }
  }

  it should "rewrite WHERE conditions based on available parameters" in {
    val examples = TableDrivenPropertyChecks.Table(
      ("Parameters", "WHERE clause"),

      (Set.empty[String], ""),

      (Set("date_range", "device_name"),
        """
          |CAST(f.time_id AS DATE) BETWEEN ?date_range
          |AND d.device_name IN ?device_name
        """
      ),

      (Set("technology", "affiliate_region", "publisher_countries", "network_name"),
        """
          |t.country_code IN ?publisher_countries
          |AND z.technology IN ?technology
          |AND t.country_level_1_name IN ?affiliate_region
          |AND z.network_name IN ?network_name
        """
      )
    )

    TableDrivenPropertyChecks.forAll(examples) {
      case (parameters, expectedSQL) =>
        olapQuery.right.map(q =>
          OlapQuery.rewriteWhereCondition(q.query.select, parameters).map(_.toSQL).getOrElse("")
        ) should be (Right(expectedSQL.stripMargin.trim))
    }
  }

  it should "rewrite GROUP BY expressions for given dimensions" in {
    val examples = TableDrivenPropertyChecks.Table(
      ("Dimensions", "GROUP BY clause"),

      (Set.empty[String], ""),

      (Set("hour"),
        """
          |ROLLUP(
          |  (
          |    DATE_TRUNC('hour', f.time_id)
          |  )
          |)
        """
      ),

      (Set("affiliate_name", "affiliate_country"),
        """
          |ROLLUP(
          |  (
          |    z.affiliate_name,
          |    t.country_name
          |  )
          |)
        """
      ),

      (Set("affiliate_country", "affiliate_name", "network_name", "zone_name", "day"),
        """
          |ROLLUP(
          |  (
          |    z.description,
          |    z.affiliate_name,
          |    t.country_name,
          |    z.network_name,
          |    DATE_TRUNC('day', f.time_id)
          |  )
          |)
        """
      )

    )

    TableDrivenPropertyChecks.forAll(examples) {
      case (dimensions, expectedSQL) =>
        olapQuery.right.flatMap(q =>
          dimensions.foldRight(Right(Nil):Either[Err,List[Expression]]) {
            (d, acc) => for(a <- acc.right; b <- q.getProjection(d).right.map(_.expression).right) yield b :: a
          }.right.map(expressions => OlapQuery.rewriteGroupBy(q.query.select, expressions).map(_.toSQL).mkString(",\n"))
        ) should be (Right(expectedSQL.stripMargin.trim))
    }
  }

  it should "compute the right query" in {
    val examples = TableDrivenPropertyChecks.Table(
      ("Projection", "Selection", "SQL"),

      (
        OlapProjection(Set("affiliate_country"), Set("displays", "clicks")),
        OlapSelection(Map.empty, Map.empty),
        """
        |SELECT
        |  t.country_name AS affiliate_country,
        |  SUM(displays) AS displays,
        |  SUM(clicks) AS clicks
        |FROM
        |  wopr.fact_zone_device_stats_hourly AS f
        |  JOIN wopr.dim_country AS t
        |    ON t.country_name = t2.country_name
        |  JOIN wopr.dim_country AS t2
        |    ON t2.country_id = f.affiliate_country_id
        |GROUP BY
        |  ROLLUP(
        |    (
        |      t.country_name
        |    )
        |  )
        """
      ),

      (
        OlapProjection(Set("affiliate_country", "day"), Set("displays", "clicks", "marketplace_revenue")),
        OlapSelection(
          Map(
            "currency_id" -> 1,
            "publisher_countries" -> List("FR", "UK", "US", "DE"),
            "date_range" -> ("2015-09-10","2015-09-11")
          ),
          Map(
            "affiliate_country" -> List("FRANCE", "GERMANY"),
            "device_name" -> List("IPAD", "IPHONE")
          )
        ),
        """
        |SELECT
        |  t.country_name AS affiliate_country,
        |  DATE_TRUNC('day', f.time_id) AS day,
        |  SUM(displays) AS displays,
        |  SUM(clicks) AS clicks,
        |  SUM(marketplace_revenue_euro * r.rate) AS marketplace_revenue
        |FROM
        |  wopr.fact_zone_device_stats_hourly AS f
        |  JOIN wopr.dim_device AS d
        |    ON d.device_id = f.device_id
        |  JOIN wopr.dim_country AS t
        |    ON t.country_name = t2.country_name
        |  JOIN wopr.fact_euro_rates_hourly AS r
        |    ON r.currency_id = 1
        |    AND f.time_id = r.time_id
        |  JOIN wopr.dim_country AS t2
        |    ON t2.country_id = f.affiliate_country_id
        |WHERE
        |  CAST(f.time_id AS DATE) BETWEEN '2015-09-10' AND '2015-09-11'
        |  AND t.country_code IN ('FR', 'UK', 'US', 'DE')
        |  AND d.device_name IN ('IPAD', 'IPHONE')
        |  AND t.country_name IN ('FRANCE', 'GERMANY')
        |GROUP BY
        |  ROLLUP(
        |    (
        |      t.country_name,
        |      DATE_TRUNC('day', f.time_id)
        |    )
        |  )
        |ORDER BY
        |  DATE_TRUNC('day', f.time_id)
        """
      )
    )

    TableDrivenPropertyChecks.forAll(examples) {
      case (projection, selection, expectedSQL) =>
        olapQuery.right.flatMap(
          _.computeQuery(projection, selection)
        ) should be (Right(expectedSQL.stripMargin.trim))
    }
  }

  it should "rewrite metrics aggregate" in {
    val examples = TableDrivenPropertyChecks.Table(
      ("Metric", "Aggregate"),

      ("clicks", Some(
        SumPostAggregate(
          FunctionCallExpression("sum", None, args = List(
            ColumnExpression(ColumnIdent("clicks", None))
          ))
        )
      )),

      ("tac", Some(
        SumPostAggregate(
          FunctionCallExpression("sum", None, args = List(
            MathExpression(
              "*",
              ColumnExpression(ColumnIdent("tac_euro", None)),
              ColumnExpression(ColumnIdent("rate", Some(TableIdent("r", None))))
            )
          ))
        )
      )),

      ("ctr", Some(
        DividePostAggregate(
          SumPostAggregate(
            FunctionCallExpression("sum", None, args = List(
              ColumnExpression(ColumnIdent("clicks", None))
            ))
          ),
          SumPostAggregate(
            FunctionCallExpression("sum", None, args = List(
              ColumnExpression(ColumnIdent("displays", None))
            ))
          )
        )
      ))

    )

    TableDrivenPropertyChecks.forAll(examples) {
      case (metric, aggregateExpression) =>
        olapQuery.right.flatMap(_.rewriteMetricAggregate(metric)) should be (Right(aggregateExpression))
    }
  }

  it should "work with a subselect as relation" in {
    val QUERY =
    """
      SELECT
        device as device,
        zone as zone,
        affiliate as affiliate,
        affiliate_country as affiliate_country,
        affiliate_region as affiliate_region,
        network as network,

        hour as hour,
        day as day,
        week as week,
        month as month,
        quarter as quarter,

        SUM(displays) as displays,
        SUM(clicks) as clicks,
        SUM(sales) as sales,
        SUM(order_value_euro * r.rate) as order_value,
        SUM(revenue_euro * r.rate) as revenue,
        SUM(tac_euro * r.rate) as tac,
        SUM((revenue_euro - tac_euro) * r.rate) as revenue_ex_tac,
        SUM(marketplace_revenue_euro * r.rate) as marketplace_revenue,
        SUM((marketplace_revenue_euro - tac_euro) * r.rate) as marketplace_revenue_ex_tac,
        ZEROIFNULL(SUM(clicks)/NULLIFZERO(SUM(displays))) as ctr,
        ZEROIFNULL(SUM(sales)/NULLIFZERO(SUM(clicks))) as cr,
        ZEROIFNULL(SUM(revenue_euro - tac_euro)/NULLIFZERO(SUM(revenue_euro))) as margin,
        ZEROIFNULL(SUM(marketplace_revenue_euro - tac_euro)/NULLIFZERO(SUM(marketplace_revenue_euro))) as marketplace_margin,
        ZEROIFNULL(SUM(revenue_euro * r.rate)/NULLIFZERO(SUM(clicks))) as cpc,
        ZEROIFNULL(SUM(tac_euro * r.rate)/NULLIFZERO(SUM(displays))) * 1000 as cpm

      FROM
        (
          SELECT
            date_trunc('month', time_id) AS x,

            device_id as device,
            zone_id as zone,
            affiliate_id as affiliate,
            affiliate_country_id as affiliate_country,
            affiliate_region_name as affiliate_region,
            network_id as network,

            date_trunc('hour', time_id) as hour,
            date_trunc('day', time_id) as day,
            date_trunc('week', time_id) as week,
            date_trunc('month', time_id) as month,
            date_trunc('quarter', time_id) as quarter,

            SUM(displays) as displays,
            SUM(clicks) as clicks,
            SUM(sales) as sales,
            SUM(order_value_euro) as order_value_euro,
            SUM(revenue_euro) as revenue_euro,
            SUM(tac_euro) as tac_euro,
            SUM(marketplace_revenue_euro) as marketplace_revenue_euro

          FROM
            wopr.fact_portfolio

          WHERE
            CAST(time_id AS DATE) BETWEEN ?[day)
            AND affiliate_country_id IN ?{publisher_countries}
            AND device_id IN ?{device}
            AND zone_id IN ?{zone}
            AND affiliate_id IN ?{affiliate}
            AND affiliate_country_id IN ?{affiliate_country}
            AND affiliate_region_name IN ?{affiliate_region}
            AND network_id IN ?{network}

          GROUP BY
            device_id,
            zone_id,
            affiliate_id,
            affiliate_country_id,
            network_id,
            affiliate_region_name,
            date_trunc('hour', time_id),
            date_trunc('day', time_id),
            date_trunc('week', time_id),
            date_trunc('month', time_id),
            date_trunc('quarter', time_id)
        ) as f
        JOIN wopr.fact_euro_rates_hourly as r
          ON r.currency_id = ?currency_id AND x = r.time_id

      GROUP BY ROLLUP((
        device,
        zone,
        affiliate,
        affiliate_country,
        network,
        affiliate_region,
        hour,
        day,
        week,
        month,
        quarter
      ))

      ORDER BY
        hour,
        day,
        week,
        month,
        quarter
    """

    val olapQuery = VizSQL.parseOlapQuery(QUERY, BIDATA).left.map(e => sys.error(e.toString(QUERY))).right.get

    olapQuery.getTimeDimensions should be (Right(List(
      "hour",
      "day",
      "week",
      "month",
      "quarter"
    )))

    olapQuery.getDimensions should be (Right(List(
      "device",
      "zone",
      "affiliate",
      "affiliate_country",
      "affiliate_region",
      "network"
    )))

    olapQuery.getMetrics should be (Right(List(
      "displays",
      "clicks",
      "sales",
      "order_value",
      "revenue",
      "tac",
      "revenue_ex_tac",
      "marketplace_revenue",
      "marketplace_revenue_ex_tac",
      "ctr",
      "cr",
      "margin",
      "marketplace_margin",
      "cpc",
      "cpm"
    )))

    olapQuery.getParameters should be (Right(List(
      "day",
      "publisher_countries",
      "device",
      "zone",
      "affiliate",
      "affiliate_country",
      "affiliate_region",
      "network",
      "currency_id"
    )))

    olapQuery.computeQuery(
      OlapProjection(Set("device"), Set("clicks")),
      OlapSelection(Map("currency_id" -> 1, "publisher_countries" -> List(1,2,3), "day" -> ("2015-09-10","2015-09-11")), Map.empty)
    ).left.map(_.toString(QUERY)) should be (Right(
      """
      |SELECT
      |  device AS device,
      |  SUM(clicks) AS clicks
      |FROM
      |  (
      |    SELECT
      |      device_id AS device,
      |      SUM(clicks) AS clicks
      |    FROM
      |      wopr.fact_portfolio
      |    WHERE
      |      CAST(time_id AS DATE) BETWEEN '2015-09-10' AND '2015-09-11'
      |      AND affiliate_country_id IN (1, 2, 3)
      |    GROUP BY
      |      device_id
      |  ) AS f
      |GROUP BY
      |  ROLLUP(
      |    (
      |      device
      |    )
      |  )
      """.stripMargin.trim
    ))

    olapQuery.computeQuery(
      OlapProjection(Set("device", "hour"), Set("clicks")),
      OlapSelection(Map("currency_id" -> 1, "publisher_countries" -> List(1,2,3), "day" -> ("2015-09-10","2015-09-11")), Map.empty)
    ).left.map(_.toString(QUERY)) should be (Right(
      """
      |SELECT
      |  device AS device,
      |  hour AS hour,
      |  SUM(clicks) AS clicks
      |FROM
      |  (
      |    SELECT
      |      device_id AS device,
      |      DATE_TRUNC('hour', time_id) AS hour,
      |      SUM(clicks) AS clicks
      |    FROM
      |      wopr.fact_portfolio
      |    WHERE
      |      CAST(time_id AS DATE) BETWEEN '2015-09-10' AND '2015-09-11'
      |      AND affiliate_country_id IN (1, 2, 3)
      |    GROUP BY
      |      device_id,
      |      DATE_TRUNC('hour', time_id)
      |  ) AS f
      |GROUP BY
      |  ROLLUP(
      |    (
      |      device,
      |      hour
      |    )
      |  )
      |ORDER BY
      |  hour
      """.stripMargin.trim
    ))

    olapQuery.computeQuery(
      OlapProjection(Set("device", "hour"), Set("clicks", "marketplace_revenue")),
      OlapSelection(Map("currency_id" -> 1, "publisher_countries" -> List(1,2,3), "day" -> ("2015-09-10","2015-09-11")), Map.empty)
    ).left.map(_.toString(QUERY)) should be (Right(
      """
      |SELECT
      |  device AS device,
      |  hour AS hour,
      |  SUM(clicks) AS clicks,
      |  SUM(marketplace_revenue_euro * r.rate) AS marketplace_revenue
      |FROM
      |  (
      |    SELECT
      |      DATE_TRUNC('month', time_id) AS x,
      |      device_id AS device,
      |      DATE_TRUNC('hour', time_id) AS hour,
      |      SUM(clicks) AS clicks,
      |      SUM(marketplace_revenue_euro) AS marketplace_revenue_euro
      |    FROM
      |      wopr.fact_portfolio
      |    WHERE
      |      CAST(time_id AS DATE) BETWEEN '2015-09-10' AND '2015-09-11'
      |      AND affiliate_country_id IN (1, 2, 3)
      |    GROUP BY
      |      device_id,
      |      DATE_TRUNC('hour', time_id),
      |      DATE_TRUNC('month', time_id)
      |  ) AS f
      |  JOIN wopr.fact_euro_rates_hourly AS r
      |    ON r.currency_id = 1
      |    AND x = r.time_id
      |GROUP BY
      |  ROLLUP(
      |    (
      |      device,
      |      hour
      |    )
      |  )
      |ORDER BY
      |  hour
      """.stripMargin.trim
    ))
  }

}