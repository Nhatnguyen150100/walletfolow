# ADR-002 — Saga theo mô hình orchestration

**Trạng thái:** Accepted · **Ngày:** 2026-08-12 · **Milestone:** 3

## Context

Vì mỗi service có database riêng (ADR-001), một giao dịch nạp/rút tiền chạm nhiều service và nhiều
database. Không có transaction phân tán (2PC không khả thi về vận hành và không mở rộng được), nên
cần một cơ chế đạt "all-or-nothing" ở mức **nghiệp vụ**.

Hai mô hình Saga:
- **Choreography:** mỗi service nghe event và tự quyết định bước tiếp theo. Không có điểm điều phối.
- **Orchestration:** một service làm "nhạc trưởng", ra lệnh từng bước và quyết định khi nào bù trừ.

## Decision

Dùng **orchestration**, với `transaction-service` là orchestrator.

Lý do quyết định là **khả năng trả lời câu hỏi "giao dịch này đang ở đâu?"**. Với choreography,
trạng thái của một giao dịch nằm rải rác trong log của năm service và không ai biết toàn cục; muốn
biết nó đang ở bước nào phải ghép lại từ nhiều nguồn. Với một hệ thống tiền — nơi câu hỏi đó được
đặt ra mỗi khi có khiếu nại, mỗi khi đối soát lệch, mỗi khi điều tra sự cố — đó là cái giá quá đắt.

Orchestration cho phép lưu `saga_state` bền vững: bước hiện tại, dữ liệu cần để bù trừ, deadline.
Nhờ đó saga **resume được sau crash** và **phát hiện được saga treo**.

## Consequences

- ✅ Trạng thái giao dịch nằm một chỗ, truy vấn được, audit được.
- ✅ Thứ tự bù trừ tường minh (ngược thứ tự tiến).
- ✅ Phát hiện saga treo bằng `deadline_at` — không có điều này thì giao dịch treo là vô hình.
- ⚠️ `transaction-service` thành điểm tập trung: nó chết thì giao dịch mới không khởi tạo được
  (giao dịch đang chạy vẫn resume được nhờ `saga_state` bền vững).
- ⚠️ Mỗi bước phải **idempotent** — orchestrator có quyền retry bất kỳ bước nào.
- ⚠️ Bù trừ phải **luôn thành công**; thất bại nhiều lần thì đẩy vào `MANUAL_REVIEW` + alert. Đây
  là van an toàn cuối, không phải tính năng phụ.

## Liên quan
ADR-001, ADR-003, master-plan §7.4 §9, roadmap §2.4
