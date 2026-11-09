package com.manas.lakehouse.contract

/**
 * One canonical field's declared contract, checked against a feed's raw column before
 * harmonization. `sourceColumn` is null when this feed does not supply the canonical
 * field at all (that field is simply absent/null for rows from this feed after
 * harmonization, which is itself part of the "feeds disagree" story, not an error).
 */
data class FieldContract(
    val canonicalName: String,
    val sourceColumn: String?,
    val type: FieldType,
    val required: Boolean = true,
    val minValue: Double? = null,
    val maxValue: Double? = null,
    val allowedValues: List<String>? = null,
)

enum class FieldType { STRING, DECIMAL, DATE, TIMESTAMP }

/** One feed's declared schema contract: which raw columns it carries and what each must satisfy. */
data class FeedContract(
    val feedName: String,
    val canonicalTable: String, // "positions" | "cash" | "nav"
    val rowIdColumn: String,
    val fields: List<FieldContract>,
) {
    fun suppliedFields(): List<FieldContract> = fields.filter { it.sourceColumn != null }
}

data class ContractViolation(
    val feedName: String,
    val sourceRowId: String,
    val column: String,
    val reason: String,
)
