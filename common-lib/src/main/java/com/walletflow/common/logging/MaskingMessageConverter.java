package com.walletflow.common.logging;

import ch.qos.logback.classic.pattern.MessageConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * {@code %m} của logback nhưng đã che dữ liệu nhạy cảm ({@link SensitiveDataMasker}). Khai trong
 * {@code walletflow/logging/masking-console.xml} với tên {@code %maskedMsg}.
 */
public class MaskingMessageConverter extends MessageConverter {

    @Override
    public String convert(ILoggingEvent event) {
        return SensitiveDataMasker.mask(super.convert(event));
    }
}
