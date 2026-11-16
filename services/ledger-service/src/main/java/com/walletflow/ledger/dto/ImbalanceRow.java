package com.walletflow.ledger.dto;

import java.util.UUID;

/**
 * Tổng posting của MỘT nhóm {@code (tenant, currency)} — nguồn của đối soát tầng 1
 * (roadmap 3.1, ADR-023 §2).
 *
 * <p>Là interface projection chứ không phải {@code record} như các DTO khác trong gói này: truy vấn
 * sinh ra nó là native (phải {@code JOIN accounts} để lấy {@code currency}, vốn không có ở
 * {@code postings} lẫn {@code journal_entries}), và native query không dựng được record qua
 * constructor như JPQL. Tên getter phải khớp alias cột trong câu truy vấn.
 *
 * <p>Tên có chữ "Imbalance" nhưng một hàng ở đây <b>chưa chắc</b> lệch: truy vấn trả về MỌI nhóm,
 * việc lọc {@code total <> 0} làm trong Java. Cố ý — xem {@code PostingRepository.sumByTenantAndCurrency}.
 */
public interface ImbalanceRow {

    UUID getTenantId();

    String getCurrency();

    long getTotalMinor();
}
