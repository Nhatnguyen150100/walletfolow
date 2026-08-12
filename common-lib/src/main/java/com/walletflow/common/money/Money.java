package com.walletflow.common.money;

import java.util.Objects;

/**
 * Money — value object biểu diễn một khoản tiền, GIẢI QUYẾT BÀI TOÁN P8 (float làm sai tiền).
 *
 * <p><b>Nguyên tắc bất di bất dịch:</b>
 * <ul>
 *   <li>Lưu bằng <b>đơn vị nhỏ nhất</b> (minor unit) kiểu số nguyên {@code long} — với VND
 *       minor unit là 1 đồng, với USD là 1 cent. TUYỆT ĐỐI không dùng {@code double}/{@code float}
 *       cho tiền (0.1 + 0.2 != 0.3).</li>
 *   <li>Mọi phép cộng/trừ dùng {@link Math#addExact}/{@link Math#subtractExact} để <b>ném lỗi khi
 *       tràn số</b> thay vì im lặng cho ra kết quả sai.</li>
 *   <li>Không cho cộng/trừ hai loại tiền khác nhau.</li>
 * </ul>
 *
 * <p><b>Cho phép giá trị ÂM một cách có chủ đích:</b> hệ thống dùng sổ cái double-entry, nơi một
 * bút toán ghi nợ (debit) là số âm và ghi có (credit) là số dương. Vì vậy Money mang dấu. Ở tầng
 * API, khi nhận số tiền giao dịch từ client, hãy gọi {@link #requirePositive()} để chặn số ≤ 0.
 */
public record Money(long amountMinor, String currency) implements Comparable<Money> {

  public Money {
    Objects.requireNonNull(currency, "currency không được null");
    currency = currency.trim().toUpperCase();
    if (currency.length() != 3) {
      throw new IllegalArgumentException("currency phải là mã ISO-4217 gồm 3 ký tự, nhận: " + currency);
    }
  }

  public static Money of(long amountMinor, String currency) {
    return new Money(amountMinor, currency);
  }

  public static Money zero(String currency) {
    return new Money(0L, currency);
  }

  public Money plus(Money other) {
    requireSameCurrency(other);
    return new Money(Math.addExact(amountMinor, other.amountMinor), currency);
  }

  public Money minus(Money other) {
    requireSameCurrency(other);
    return new Money(Math.subtractExact(amountMinor, other.amountMinor), currency);
  }

  /** Đảo dấu — dùng để sinh bút toán đối ứng (một bên nợ, một bên có). */
  public Money negate() {
    return new Money(Math.negateExact(amountMinor), currency);
  }

  public Money abs() {
    return amountMinor < 0 ? negate() : this;
  }

  public boolean isZero() {
    return amountMinor == 0;
  }

  public boolean isPositive() {
    return amountMinor > 0;
  }

  public boolean isNegative() {
    return amountMinor < 0;
  }

  /** {@code this >= other} (cùng loại tiền). */
  public boolean isGreaterThanOrEqual(Money other) {
    requireSameCurrency(other);
    return amountMinor >= other.amountMinor;
  }

  /** Chặn số tiền ≤ 0 ở tầng API (số tiền giao dịch phải dương). */
  public Money requirePositive() {
    if (!isPositive()) {
      throw new IllegalArgumentException("Số tiền phải lớn hơn 0, nhận: " + amountMinor);
    }
    return this;
  }

  private void requireSameCurrency(Money other) {
    Objects.requireNonNull(other, "other không được null");
    if (!currency.equals(other.currency)) {
      throw new IllegalArgumentException(
          "Không thể thao tác trên hai loại tiền khác nhau: " + currency + " vs " + other.currency);
    }
  }

  @Override
  public int compareTo(Money other) {
    requireSameCurrency(other);
    return Long.compare(amountMinor, other.amountMinor);
  }

  @Override
  public String toString() {
    return amountMinor + " " + currency + " (minor)";
  }
}
