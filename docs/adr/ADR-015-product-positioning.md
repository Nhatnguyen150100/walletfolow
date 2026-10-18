# ADR-015 — Định vị sản phẩm: ví đóng (A) trước, kiến trúc Wallet-as-a-Service (B)

**Trạng thái:** Accepted · **Ngày:** 2026-10-18 · **Milestone:** đóng Phase 1, trước Phase 2

## Context

Roadmap §4 đặt ra ba con đường, và ghi rõ phải chốt **trước Phase 2**, vì chúng đòi hỏi những thứ
rất khác nhau ở Phase 2 và Phase 5:

| | A — Ví đóng (closed-loop) | B — Wallet-as-a-Service | C — Ví có giấy phép cho người dùng cuối |
|---|---|---|---|
| Tiền đi đâu | Chỉ tiêu trong hệ sinh thái của chính mình | Bán engine cho doanh nghiệp | Chuyển cho bên thứ ba bất kỳ |
| Ai cầm tiền | Mình, như tiền trả trước cho hàng hoá/dịch vụ của mình | Khách hàng doanh nghiệp | Mình — cần giấy phép TGTT |
| Thêm gì ở Phase 5 | 5.3 KYC/AML/chống gian lận (bắt buộc); 5.5 giấy phép **nếu** cho rút ra ngoài | 5.1 API key + HMAC, webhook, SDK, sandbox, tài liệu công khai, SLO | Giấy phép, vốn pháp định, eKYC/AML |

Đến lúc này mới chỉ có **khuyến nghị**, chưa thành quyết định:
- Roadmap §4 khuyến nghị "đi A trước như khách hàng đầu tiên của chính mình, kiến trúc theo B".
- ADR-010 mở đầu bằng "Roadmap §4 chọn con đường B".
- Phase 1 đã xây theo B: `tenant_id` trên mọi bảng và mọi event, tenant chỉ lấy từ JWT (ADR-010), tài
  khoản hệ thống cấp theo tenant (ADR-011).

Phase 2 (nạp/rút qua cổng thanh toán thật) phụ thuộc trực tiếp vào việc chạy thật theo con đường nào.

## Decision

**Chạy theo con đường A trước, làm khách hàng đầu tiên của chính mình. Kiến trúc tiếp tục theo B.**

- **A là cách hệ thống được dùng thật đầu tiên:**
  - Một tenant: chính sản phẩm của mình.
  - Tiền nạp vào là trả trước, chỉ tiêu được trong hệ sinh thái của mình.
  - Nạp tiền đi qua **một cổng thanh toán đã có giấy phép** (Phase 2.2). Hệ thống không trực tiếp
    nhận tiền của bên thứ ba qua kênh ngân hàng riêng.
- **B là hướng kiến trúc, không đổi so với Phase 1:**
  - Mọi thiết kế mới vẫn multi-tenant và tenant chỉ đến từ nguồn đáng tin (ADR-010).
  - Không thêm giả định "chỉ có một tenant" vào code.
  - Phần riêng của B — API key, webhook cho khách hàng, SDK, sandbox — để ở Phase 5.1.
- **C bị loại:** đó là bài toán pháp nhân và vốn, không phải bài toán kỹ thuật (roadmap §4).

## Hệ quả cho các phase sau

- **Phase 2 — nạp tiền:**
  - Làm 2.2 bank-adapter, 2.3 webhook ingestion, và phần **top-up** của 2.4 như roadmap.
  - **Nhà cung cấp cụ thể** cho 2.2 (VNPay / MoMo / Casso / Stripe…) chốt khi bắt đầu 2.2, là một ADR
    riêng.
- **Phase 2 — rút tiền ra ngoài: CHƯA chốt.** Gồm phần **withdraw** của 2.4, và 2.1 HOLD (HOLD tồn tại
  để phục vụ luồng rút).
  - Roadmap §4 cảnh báo: ví đóng mà cho rút tiền mặt ra bên ngoài thì về bản chất đã là dịch vụ thanh
    toán, và kéo theo 5.5 giấy phép.
  - Trước khi làm phần rút phải trả lời — **bằng kiểm tra pháp lý, không bằng code** — một trong hai:
    - (a) chỉ hoàn tiền về đúng nguồn đã nạp;
    - (b) rút tự do.
  - Đến lúc đó phần rút bị chặn. ADR này không phải ý kiến pháp lý.
- **Phase 5 — với A, 5.3 KYC/AML/chống gian lận là bắt buộc** trước khi chạy với người dùng thật ở quy
  mô lớn (roadmap 5.3); 5.5 giấy phép chỉ phát sinh nếu chọn (b).
- **Roadmap 1.4 notification-service bị cắt khỏi Phase 1.** Với B, khách hàng doanh nghiệp nhận kết quả
  giao dịch qua **webhook** (Phase 5.1), không qua một service thông báo riêng.
  - Nguồn cho webhook là event `txn.event.completed.v1` / `txn.event.failed.v1` (ADR-014).
  - Mỗi lần chuyển sang `COMPLETED` / `FAILED`, saga ghi một event vào outbox với **eventId tất định**.
  - Outbox giao **at-least-once**, nên bên tiêu thụ phải khử trùng theo `eventId`.
  - Bên tiêu thụ phải xử lý được ca `FAILED` rồi `COMPLETED` cho cùng một giao dịch (ledger thắng,
    ADR-014).
  - Chuyển sang `MANUAL_REVIEW` **không** phát event nào.
- **Không đổi:** mọi bất biến ở roadmap §5 và CLAUDE.md.

## Alternatives rejected

- **Chỉ B, không chạy A:** mất "khách hàng đầu tiên" để kiểm chứng engine trên tiền thật trước khi bán
  cho doanh nghiệp. Phase 5.1 cũng là phần dài nhất (~30 ngày công) — đi thẳng vào đó là trì hoãn lần
  chạy thật đầu tiên.
- **Chỉ A, bỏ kiến trúc B:** phải gỡ `tenant_id` khỏi mọi bảng/event, hoặc giữ nó mà không ai bảo
  vệ. Khi quay lại B thì phải thêm tenant trên dữ liệu thật — roadmap §4 ước tính 1 tuần + một đêm
  downtime.
- **Để mở thêm:** Phase 2 bị chặn, và quyết định cứ tồn tại ngầm trong ADR-010 — đúng thứ ADR sinh ra
  để tránh.

## Liên quan

- Roadmap §4 (ba con đường), §5 (bất biến), Phase 2 (2.1, 2.2, 2.4), Phase 5 (5.1, 5.3, 5.5).
- ADR-010 (lan truyền tenant), ADR-011 (tài khoản hệ thống theo tenant), ADR-014 (event kết quả giao
  dịch).
