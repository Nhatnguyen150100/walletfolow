package com.walletflow.wallet.client;

import com.walletflow.common.exception.AppException;
import org.springframework.http.HttpStatus;

/**
 * Không đọc được sổ cái — rebuild không chạy, read model giữ nguyên. Thử lại sau là đúng.
 *
 * <p>Thông điệp cố định: chi tiết kỹ thuật được ghi log ở nơi ném, không trả về người gọi.
 */
public class LedgerUnavailableException extends AppException {
    public LedgerUnavailableException() {
        super(HttpStatus.SERVICE_UNAVAILABLE, "LEDGER_UNAVAILABLE",
                "Tạm thời không đọc được sổ cái, vui lòng thử lại sau");
    }
}
