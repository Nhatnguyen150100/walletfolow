package com.walletflow.common.logging;

import ch.qos.logback.classic.pattern.ExtendedThrowableProxyConverter;
import ch.qos.logback.classic.spi.IThrowableProxy;

/**
 * Stack trace đã che dữ liệu nhạy cảm ({@code %maskedEx}). Che riêng phần exception vì đó là đường lọt hay
 * gặp nhất: message của lỗi HTTP client thường chứa nguyên URL — với cổng thanh toán là URL có chữ ký.
 */
public class MaskingThrowableConverter extends ExtendedThrowableProxyConverter {

    @Override
    protected String throwableProxyToString(IThrowableProxy proxy) {
        return SensitiveDataMasker.mask(super.throwableProxyToString(proxy));
    }
}
