package com.walletflow.common.money;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test cho Money value object — chứng minh cách biểu diễn tiền là ĐÚNG (bài toán P8).
 * Đây là deliverable test đầu tiên của dự án: "tiền không bao giờ sai vì float/tràn số/lẫn loại tiền".
 */
class MoneyTest {

  @Test
  @DisplayName("Cộng trừ chính xác trên số nguyên minor unit (không dính lỗi float)")
  void addAndSubtract() {
    Money a = Money.of(10, "VND");   // ứng với 0.10 nếu là USD cent — nhưng ta so số nguyên
    Money b = Money.of(20, "VND");
    assertThat(a.plus(b)).isEqualTo(Money.of(30, "VND"));   // 10 + 20 = 30, tuyệt đối chính xác
    assertThat(b.minus(a)).isEqualTo(Money.of(10, "VND"));
  }

  @Test
  @DisplayName("Chặn cộng/trừ hai loại tiền khác nhau")
  void rejectDifferentCurrency() {
    Money vnd = Money.of(1000, "VND");
    Money usd = Money.of(1000, "USD");
    assertThatThrownBy(() -> vnd.plus(usd))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("khác nhau");
  }

  @Test
  @DisplayName("Ném lỗi khi tràn số thay vì im lặng cho kết quả sai")
  void overflowThrows() {
    Money big = Money.of(Long.MAX_VALUE, "VND");
    assertThatThrownBy(() -> big.plus(Money.of(1, "VND")))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  @DisplayName("negate() sinh giá trị đối ứng cho bút toán double-entry (nợ/có)")
  void negateForDoubleEntry() {
    Money credit = Money.of(100_000, "VND");
    Money debit = credit.negate();
    assertThat(debit.amountMinor()).isEqualTo(-100_000);
    // Bất biến double-entry: một bút toán gồm nợ + có thì tổng = 0
    assertThat(credit.plus(debit).isZero()).isTrue();
  }

  @Test
  @DisplayName("requirePositive() chặn số tiền <= 0 ở tầng API")
  void requirePositive() {
    assertThat(Money.of(1, "VND").requirePositive().amountMinor()).isEqualTo(1);
    assertThatThrownBy(() -> Money.of(0, "VND").requirePositive())
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Money.of(-5, "VND").requirePositive())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("So sánh số dư khả dụng: isGreaterThanOrEqual")
  void compareBalance() {
    Money balance = Money.of(50_000, "VND");
    assertThat(balance.isGreaterThanOrEqual(Money.of(50_000, "VND"))).isTrue();
    assertThat(balance.isGreaterThanOrEqual(Money.of(80_000, "VND"))).isFalse();
  }

  @Test
  @DisplayName("Chuẩn hoá & kiểm tra mã tiền tệ ISO-4217 (3 ký tự)")
  void currencyNormalisation() {
    assertThat(Money.of(1, "vnd").currency()).isEqualTo("VND");
    assertThatThrownBy(() -> Money.of(1, "VNDX"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
