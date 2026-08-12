# ADR-005 — Biểu diễn tiền bằng số nguyên (minor unit)

**Trạng thái:** Accepted · **Ngày:** 2026-08-12 · **Milestone:** 0

## Context (bối cảnh)

Hệ thống xử lý tiền thật. Kiểu `double`/`float` biểu diễn số thập phân theo cơ số 2 nên
không lưu chính xác nhiều giá trị thập phân cơ số 10: `0.1 + 0.2 == 0.30000000000000004`.
Dùng float cho tiền dẫn tới sai lệch tích luỹ, không thể kiểm toán — không chấp nhận được trong fintech.

## Decision (quyết định)

- Lưu mọi khoản tiền bằng **số nguyên `long` theo đơn vị nhỏ nhất (minor unit)**: VND = 1 đồng,
  USD = 1 cent. Cột DB kiểu `BIGINT`.
- Bọc trong **value object bất biến `Money(amountMinor, currency)`** (`common-lib`):
  - Phép cộng/trừ dùng `Math.addExact`/`Math.subtractExact` → **ném lỗi khi tràn số** thay vì
    im lặng cho kết quả sai.
  - Chặn thao tác giữa hai loại tiền khác nhau.
  - Cho phép giá trị **âm có chủ đích** để phục vụ bút toán double-entry (debit âm / credit dương);
    tầng API gọi `requirePositive()` để chặn số tiền ≤ 0 do client gửi.
- Khi cần tính theo tỉ lệ (phí, lãi) mới dùng `BigDecimal`, rồi làm tròn về minor unit với
  `RoundingMode` khai báo tường minh.

## Consequences (hệ quả)

- ✅ Tiền chính xác tuyệt đối, kiểm toán được; lỗi tràn số lộ ra ngay thay vì âm thầm.
- ✅ Bất biến double-entry `SUM = 0` biểu diễn và kiểm chứng được (xem `MoneyTest`).
- ⚠️ Phải chú ý quy đổi minor unit khi hiển thị cho người dùng (chia theo số chữ số thập phân của
  đồng tiền). Với VND (0 chữ số thập phân) thì minor unit trùng đơn vị hiển thị.
- ⚠️ Không dùng trực tiếp toán tử `+`/`-` trên số tiền — luôn qua `Money`.

## Liên quan

- Kiểm chứng: `common-lib/.../money/MoneyTest.java`
- Bài toán P8 trong `docs/walletflow-master-plan.md`
