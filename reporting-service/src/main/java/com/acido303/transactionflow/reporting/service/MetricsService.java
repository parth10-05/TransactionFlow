package com.acido303.transactionflow.reporting.service;

import com.acido303.transactionflow.reporting.dto.CountryMapDataResponse;
import com.acido303.transactionflow.reporting.dto.CountryMetricDto;
import com.acido303.transactionflow.reporting.dto.MetricsSummaryResponse;
import com.acido303.transactionflow.reporting.entity.TransactionMetricsByCurrency;
import com.acido303.transactionflow.reporting.entity.TransactionMetricsByCountry;
import com.acido303.transactionflow.reporting.entity.TransactionMetricsByType;
import com.acido303.transactionflow.reporting.repository.HighValueTransactionRepository;
import com.acido303.transactionflow.reporting.repository.MetricsByCurrencyRepository;
import com.acido303.transactionflow.reporting.repository.MetricsByCountryRepository;
import com.acido303.transactionflow.reporting.repository.MetricsByTypeRepository;
import com.acido303.transactionflow.reporting.repository.RecentTransactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class MetricsService {

    private final MetricsByCurrencyRepository metricsByCurrencyRepository;
    private final MetricsByTypeRepository metricsByTypeRepository;
    private final MetricsByCountryRepository metricsByCountryRepository;
    private final HighValueTransactionRepository highValueTransactionRepository;
    private final RecentTransactionRepository recentTransactionRepository;

    /**
     * Returns a summary of pipeline metrics.
     *
     * <p>All amounts are denominated in <strong>USD</strong>.
     * TransactionFlow accepts only USD; non-USD records are rejected by the Spark job
     * with reason "Invalid or unsupported currency" and are never counted in
     * {@code totalAmountUsd}.
     */
    public MetricsSummaryResponse getSummary() {
        long totalTransactions    = recentTransactionRepository.count();
        long rejectedTransactions = recentTransactionRepository.findTop20ByStatusOrderByCreatedAtDesc("REJECTED").size();
        long highValueTransactions = highValueTransactionRepository.count();

        // Sum all currency metrics — in a USD-only pipeline this is always USD.
        // Legacy rows from before the USD migration are included as-is.
        List<TransactionMetricsByCurrency> currencyMetrics = metricsByCurrencyRepository.findLatestByCurrency();
        BigDecimal totalAmountUsd = currencyMetrics.stream()
                .map(m -> m.getTotalAmount() != null ? m.getTotalAmount() : BigDecimal.ZERO)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        return MetricsSummaryResponse.builder()
                .totalTransactions(totalTransactions)
                .rejectedTransactions(rejectedTransactions)
                .highValueTransactions(highValueTransactions)
                .totalAmountUsd(totalAmountUsd)
                .build();
    }

    public List<TransactionMetricsByCurrency> getLatestCurrencyMetrics() {
        return metricsByCurrencyRepository.findLatestByCurrency();
    }

    public List<TransactionMetricsByType> getLatestTypeMetrics() {
        return metricsByTypeRepository.findLatestByType();
    }

    public List<TransactionMetricsByCountry> getLatestCountryMetrics() {
        return metricsByCountryRepository.findLatestByCountry();
    }

    /**
     * Returns per-country aggregated metrics for the map view.
     *
     * <p>{@code totalAmount} and {@code averageAmount} are denominated in USD.
     * The {@code currency} field is explicitly set to {@code "USD"}.
     */
    public CountryMapDataResponse getCountryMapData() {
        List<TransactionMetricsByCountry> countryMetrics = metricsByCountryRepository.findLatestByCountry();

        List<CountryMetricDto> countries = countryMetrics.stream()
                .map(m -> CountryMetricDto.builder()
                        .countryCode(m.getCountryCode())
                        .countryName(m.getCountryName())
                        .transactionCount(m.getTransactionCount())
                        .totalAmount(m.getTotalAmount())
                        .averageAmount(m.getAverageAmount())
                        .currency("USD")     // explicitly USD; was incorrectly "EUR" before
                        .highValueCount(m.getHighValueCount())
                        .rejectedCount(m.getRejectedCount())
                        .lastTransactionAt(m.getLastTransactionAt())
                        .build())
                .collect(Collectors.toList());

        return CountryMapDataResponse.builder()
                .countries(countries)
                .build();
    }
}
