package com.manas.lakehouse.lineage

/**
 * The declarative lineage manifest: one entry per canonical output column, across all
 * three canonical tables (32 positions + 30 cash + 32 nav = 94), stating whether that
 * column's value is copied from a source feed's own column (traced to a source row via
 * that row's `source_row_id`) or generated at ingestion time (`source_feed`,
 * `source_row_id` itself, `ingested_at`, `contract_version`). `LineageChecker` is what
 * actually proves the "copied" entries are honest, by re-reading the named raw feed's row
 * and comparing values; this file is the list of 94 claims, not the proof.
 */
data class LineageEntry(val table: String, val column: String, val generated: Boolean)

object LineageManifest {

    val POSITIONS_COLUMNS = listOf(
        "account_id", "symbol", "cusip", "isin", "sedol", "asset_class", "sector", "country",
        "currency", "quantity", "cost_basis", "market_value", "unrealized_pnl", "price",
        "trade_date", "settle_date", "custodian", "multiplier", "accrued_interest", "fx_rate",
        "local_currency", "pending_qty", "unsettled_qty", "weight_pct", "duration",
        "maturity_date", "coupon_rate", "rating",
        "source_feed", "source_row_id", "ingested_at", "contract_version",
    )

    val CASH_COLUMNS = listOf(
        "account_id", "currency", "balance", "available_balance", "pending_credits",
        "pending_debits", "overdraft_limit", "interest_rate", "as_of_date", "value_date",
        "sweep_account", "settlement_currency", "custodian", "collateral_balance",
        "margin_balance", "interest_accrued", "fx_rate", "wire_pending", "check_float",
        "account_type", "beneficiary", "correspondent_bank", "iban", "swift_code",
        "reconciled_flag", "last_statement_date",
        "source_feed", "source_row_id", "ingested_at", "contract_version",
    )

    val NAV_COLUMNS = listOf(
        "account_id", "as_of_date", "nav_value", "nav_per_share", "shares_outstanding",
        "gross_asset_value", "total_liabilities", "management_fee_accrual",
        "performance_fee_accrual", "prior_nav_value", "nav_change_pct", "high_water_mark",
        "currency", "fund_class", "subscription_amount", "redemption_amount", "expense_ratio",
        "benchmark_return", "ytd_return", "mtd_return", "inception_date", "administrator",
        "audit_status", "side_pocket_value", "gate_flag", "lockup_end_date",
        "distribution_amount", "carry_accrual",
        "source_feed", "source_row_id", "ingested_at", "contract_version",
    )

    private val GENERATED = setOf("source_feed", "source_row_id", "ingested_at", "contract_version")

    val ALL: List<LineageEntry> =
        POSITIONS_COLUMNS.map { LineageEntry("positions", it, it in GENERATED) } +
            CASH_COLUMNS.map { LineageEntry("cash", it, it in GENERATED) } +
            NAV_COLUMNS.map { LineageEntry("nav", it, it in GENERATED) }

    init {
        check(POSITIONS_COLUMNS.size == 32) { "positions must have exactly 32 columns" }
        check(CASH_COLUMNS.size == 30) { "cash must have exactly 30 columns" }
        check(NAV_COLUMNS.size == 32) { "nav must have exactly 32 columns" }
        check(ALL.size == 94) { "lineage manifest must cover exactly 94 output columns" }
    }
}
