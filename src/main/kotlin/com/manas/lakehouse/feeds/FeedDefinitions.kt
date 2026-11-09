package com.manas.lakehouse.feeds

import com.manas.lakehouse.contract.FeedContract
import com.manas.lakehouse.contract.FieldContract
import com.manas.lakehouse.contract.FieldType
import com.manas.lakehouse.lineage.LineageManifest

private enum class NamingStyle { ABBREVIATED_SNAKE, CAMEL }

/** A handful of common custodian/administrator abbreviations; anything not listed keeps its canonical name. */
private val ABBREVIATIONS = mapOf(
    "account_id" to "acct_num", "quantity" to "qty", "cost_basis" to "cost",
    "market_value" to "mkt_val", "price" to "px", "trade_date" to "trd_dt",
    "settle_date" to "settle_dt", "currency" to "ccy", "balance" to "bal",
    "available_balance" to "avail_bal", "as_of_date" to "asof_dt", "nav_value" to "nav",
    "shares_outstanding" to "shrs_out", "custodian" to "cust_name",
)

private fun toCamelCase(snake: String): String =
    snake.split("_").mapIndexed { i, w -> if (i == 0) w else w[0].uppercase() + w.substring(1) }.joinToString("")

private fun rawColumnName(canonical: String, style: NamingStyle): String = when (style) {
    NamingStyle.ABBREVIATED_SNAKE -> ABBREVIATIONS[canonical] ?: canonical
    NamingStyle.CAMEL -> toCamelCase(canonical)
}

/**
 * Per canonical column, the type and the numeric/enum rule a contract enforces. Columns
 * not listed default to a required free-text string with no range check (addresses,
 * identifiers, free-text names: still required and still typed, just not numerically
 * bounded).
 */
private data class Rule(val type: FieldType, val required: Boolean = true, val min: Double? = null,
                         val max: Double? = null, val allowed: List<String>? = null)

private val RULES: Map<String, Rule> = mapOf(
    "quantity" to Rule(FieldType.DECIMAL, min = -1_000_000.0, max = 1_000_000.0),
    "cost_basis" to Rule(FieldType.DECIMAL, min = -100_000_000.0, max = 100_000_000.0),
    "market_value" to Rule(FieldType.DECIMAL, min = -100_000_000.0, max = 100_000_000.0),
    "unrealized_pnl" to Rule(FieldType.DECIMAL, required = false, min = -100_000_000.0, max = 100_000_000.0),
    "price" to Rule(FieldType.DECIMAL, min = 0.0, max = 1_000_000.0),
    "trade_date" to Rule(FieldType.DATE),
    "settle_date" to Rule(FieldType.DATE),
    "multiplier" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 100_000.0),
    "accrued_interest" to Rule(FieldType.DECIMAL, required = false, min = -1_000_000.0, max = 1_000_000.0),
    "fx_rate" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1000.0),
    "pending_qty" to Rule(FieldType.DECIMAL, required = false, min = -1_000_000.0, max = 1_000_000.0),
    "unsettled_qty" to Rule(FieldType.DECIMAL, required = false, min = -1_000_000.0, max = 1_000_000.0),
    "weight_pct" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 100.0),
    "duration" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 100.0),
    "maturity_date" to Rule(FieldType.DATE, required = false),
    "coupon_rate" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 100.0),
    "rating" to Rule(FieldType.STRING, required = false,
        allowed = listOf("AAA", "AA", "A", "BBB", "BB", "B", "CCC", "NR")),

    "balance" to Rule(FieldType.DECIMAL, min = -1_000_000_000.0, max = 1_000_000_000.0),
    "available_balance" to Rule(FieldType.DECIMAL, min = -1_000_000_000.0, max = 1_000_000_000.0),
    "pending_credits" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),
    "pending_debits" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),
    "overdraft_limit" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),
    "interest_rate" to Rule(FieldType.DECIMAL, required = false, min = -10.0, max = 100.0),
    "as_of_date" to Rule(FieldType.DATE),
    "value_date" to Rule(FieldType.DATE, required = false),
    "collateral_balance" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),
    "margin_balance" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),
    "interest_accrued" to Rule(FieldType.DECIMAL, required = false, min = -1_000_000.0, max = 1_000_000.0),
    "reconciled_flag" to Rule(FieldType.STRING, required = false, allowed = listOf("Y", "N")),
    "last_statement_date" to Rule(FieldType.DATE, required = false),

    "nav_value" to Rule(FieldType.DECIMAL, min = 0.0, max = 1_000_000_000_000.0),
    "nav_per_share" to Rule(FieldType.DECIMAL, min = 0.0, max = 1_000_000.0),
    "shares_outstanding" to Rule(FieldType.DECIMAL, min = 0.0, max = 1_000_000_000.0),
    "gross_asset_value" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000_000.0),
    "total_liabilities" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000_000.0),
    "management_fee_accrual" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),
    "performance_fee_accrual" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),
    "prior_nav_value" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000_000.0),
    "nav_change_pct" to Rule(FieldType.DECIMAL, required = false, min = -100.0, max = 1000.0),
    "high_water_mark" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000_000.0),
    "subscription_amount" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),
    "redemption_amount" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),
    "expense_ratio" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 100.0),
    "benchmark_return" to Rule(FieldType.DECIMAL, required = false, min = -100.0, max = 1000.0),
    "ytd_return" to Rule(FieldType.DECIMAL, required = false, min = -100.0, max = 1000.0),
    "mtd_return" to Rule(FieldType.DECIMAL, required = false, min = -100.0, max = 1000.0),
    "inception_date" to Rule(FieldType.DATE, required = false),
    "audit_status" to Rule(FieldType.STRING, required = false, allowed = listOf("AUDITED", "UNAUDITED", "PRELIMINARY")),
    "side_pocket_value" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),
    "gate_flag" to Rule(FieldType.STRING, required = false, allowed = listOf("Y", "N")),
    "lockup_end_date" to Rule(FieldType.DATE, required = false),
    "distribution_amount" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),
    "carry_accrual" to Rule(FieldType.DECIMAL, required = false, min = 0.0, max = 1_000_000_000.0),

    "account_id" to Rule(FieldType.STRING),
)

private fun defaultRule(canonical: String): Rule = RULES[canonical] ?: Rule(FieldType.STRING, required = false)

private fun buildFields(canonicalCols: List<String>, omit: Set<String>, style: NamingStyle): List<FieldContract> =
    canonicalCols.filterNot { it in omit }.map { canonical ->
        val rule = defaultRule(canonical)
        FieldContract(
            canonicalName = canonical,
            sourceColumn = rawColumnName(canonical, style),
            type = rule.type,
            required = rule.required,
            minValue = rule.min,
            maxValue = rule.max,
            allowedValues = rule.allowed,
        )
    }

private val NON_GENERATED_POSITIONS = LineageManifest.POSITIONS_COLUMNS.filterNot { it.startsWith("source_") || it == "ingested_at" || it == "contract_version" }
private val NON_GENERATED_CASH = LineageManifest.CASH_COLUMNS.filterNot { it.startsWith("source_") || it == "ingested_at" || it == "contract_version" }
private val NON_GENERATED_NAV = LineageManifest.NAV_COLUMNS.filterNot { it.startsWith("source_") || it == "ingested_at" || it == "contract_version" }

/** The canonical Spark SQL type each canonical column should be cast to when harmonized, regardless of feed. */
fun canonicalFieldType(canonical: String): FieldType = defaultRule(canonical).type

object FeedDefinitions {

    /** Custodian "BNY-style": abbreviated snake_case raw columns, omits duration/weight_pct. */
    val POSITIONS_FEED_A = FeedContract(
        feedName = "pos_custodian_bny",
        canonicalTable = "positions",
        rowIdColumn = "row_id",
        fields = buildFields(NON_GENERATED_POSITIONS, setOf("duration", "weight_pct"), NamingStyle.ABBREVIATED_SNAKE),
    )

    /** Custodian "StateTrust-style": camelCase raw columns, omits maturity_date/coupon_rate/rating. */
    val POSITIONS_FEED_B = FeedContract(
        feedName = "pos_custodian_statetrust",
        canonicalTable = "positions",
        rowIdColumn = "rowId",
        fields = buildFields(NON_GENERATED_POSITIONS, setOf("maturity_date", "coupon_rate", "rating"), NamingStyle.CAMEL),
    )

    val CASH_FEED_A = FeedContract(
        feedName = "cash_bank_x",
        canonicalTable = "cash",
        rowIdColumn = "row_id",
        fields = buildFields(NON_GENERATED_CASH, setOf("collateral_balance", "margin_balance"), NamingStyle.ABBREVIATED_SNAKE),
    )

    val CASH_FEED_B = FeedContract(
        feedName = "cash_bank_y",
        canonicalTable = "cash",
        rowIdColumn = "rowId",
        fields = buildFields(NON_GENERATED_CASH, setOf("wire_pending", "check_float"), NamingStyle.CAMEL),
    )

    val NAV_FEED_A = FeedContract(
        feedName = "nav_admin_p",
        canonicalTable = "nav",
        rowIdColumn = "row_id",
        fields = buildFields(NON_GENERATED_NAV, setOf("side_pocket_value", "gate_flag", "lockup_end_date"), NamingStyle.ABBREVIATED_SNAKE),
    )

    val NAV_FEED_B = FeedContract(
        feedName = "nav_admin_q",
        canonicalTable = "nav",
        rowIdColumn = "rowId",
        fields = buildFields(NON_GENERATED_NAV, setOf("high_water_mark", "distribution_amount", "carry_accrual"), NamingStyle.CAMEL),
    )

    val ALL = listOf(POSITIONS_FEED_A, POSITIONS_FEED_B, CASH_FEED_A, CASH_FEED_B, NAV_FEED_A, NAV_FEED_B)
}
