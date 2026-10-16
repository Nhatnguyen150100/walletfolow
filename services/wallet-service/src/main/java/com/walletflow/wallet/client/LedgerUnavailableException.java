package com.walletflow.wallet.client;

import com.walletflow.common.exception.AppException;
import org.springframework.http.HttpStatus;

/** Không đọc được sổ cái — rebuild không chạy, read model giữ nguyên. Thử lại sau là đúng. */
public class LedgerUnavailableException extends AppException {
    public LedgerUnavailableException(String message) {
        super(HttpStatus.SERVICE_UNAVAILABLE, "LEDGER_UNAVAILABLE", message);
    }
}
