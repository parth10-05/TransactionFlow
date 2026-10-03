package com.acido303.transactionflow.reporting.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * Summary metrics response for the TransactionFlow reporting API.
 *
 * <p>All amount fields are denominated in <strong>USD</strong>.
 * TransactionFlow is a USD-only demo pipeline — no foreign-exchange conversion
 * is implemented. {@code totalAmountUsd} represents the sum of all valid
 * transaction amounts processed since the database was last reset.
 *
 * <p>The previously separate {@code totalAmountEur} and {@code totalAmountGbp}
 * fields have been removed because the pipeline no longer accepts EUR or GBP.
 */
@Data
@Builder
public class MetricsSummaryResponse {

    private long totalTransactions;
    private long rejectedTransactions;
    private long highValueTransactions;

    /** Total of all valid transaction amounts, denominated in USD. */
    private BigDecimal totalAmountUsd;
}
