# Phase 1 — Hoàn thiện vòng tiền nội bộ: tổng kết khi đóng phase

> Tổng kết của **cả phase** (8 PR), không phải mô tả của một PR. Đóng ngày **2026-10-18**.
> Chi tiết từng mục: [roadmap §Phase 1](walletflow-production-roadmap.md) · số liệu tải:
> [`benchmarks/phase1.md`](benchmarks/phase1.md) · quyết định sản phẩm: [ADR-015](adr/ADR-015-product-positioning.md).

## Mục tiêu

`POST /api/v1/transactions/transfer` chạy end-to-end qua gateway — idempotent, có Saga, có read model
số dư — tức **ví dùng được** trong phạm vi closed-loop. Kiến trúc multi-tenant ngay từ đầu (con đường B,
roadmap §4).

---

## Definition of Done — 6/6

| # | Tiêu chí | Bằng chứng |
|---|---|---|
| 1 | `curl POST /api/v1/transactions/transfer` qua gateway → tiền chuyển, số dư 2 ví đúng | `scripts/e2e/phase1-dod.sh` (#25), chạy trong job CI `e2e` ở mọi PR — 52/52 kiểm tra PASS |
| 2 | Gửi lại cùng `Idempotency-Key` → không trừ tiền lần hai | `IdempotencyIT` + E2E: gửi lại 1 lần và 10 lần song song (body khớp từng byte); 10 request **đầu tiên** song song cùng một key mới → một giao dịch |
| 3 | Giết service giữa saga → giao dịch vẫn kết thúc đúng | E2E `docker kill` ledger rồi transaction-service giữa saga → `COMPLETED`, đúng một bút toán; `SagaFlowIT`, `StuckSagaIT` |
| 4 | Rebuild projection khớp ledger 100% | `WalletRebuildIT` + E2E: làm hỏng số dư và lịch sử → rebuild (ADMIN) khớp sổ cái |
| 5 | Tenant A không thấy được gì của tenant B | Cách ly dữ liệu: `TenantIsolationIT`, `WalletApiIT`. E2E: tenant/danh tính chỉ lấy từ token, header giả vô hiệu. Hai tenant thật qua gateway: Phase 5 |
| 6 | Có số liệu load test cơ sở | [`benchmarks/phase1.md`](benchmarks/phase1.md) (#26): chịu bền 20 TPS, gãy ở 40 TPS, công suất bền vững ~25 TPS do outbox relay, tiền đúng dưới tải |

---

## 🐛 Những gì quá trình kiểm chứng phát hiện

Cùng tinh thần Phase 0: phần lớn thuộc loại **trông như đúng mà thật ra sai** — nguy hiểm hơn lỗi làm vỡ
build.

1. **api-gateway chưa từng khởi động được trong compose** — từ commit đầu tới 1.5a.
   - Nguyên nhân: hai bean `KeyResolver` không có `@Primary`.
   - Vì sao không lộ: gateway chỉ có unit test, CI chỉ build image chứ không chạy.
   - E2E (#25) là lần đầu có request đi qua gateway thật. Đã sửa, kèm `ApiGatewayApplicationTest`
     (fail trước khi sửa).
2. **Gateway khởi động trước auth-service thì token hợp lệ đầu tiên bị 401.**
   - Lần nạp JWKS lúc khởi động hỏng, gateway giữ 0 khoá, và chỉ nạp lại sau khi một request bị từ
     chối.
   - **Chưa sửa** — roadmap 4.5.
3. **Hai lỗi phương pháp đo**, cả hai cho số "đẹp" nhưng sai (#26):
   - Throughput chia cho cả lượt luôn ra cùng một con số (~21 TPS), vì bị lịch tải chặn trên.
   - Lượt đo chạy chồng lên outbox chưa xả của lượt trước, nên đo nhầm độ trễ của lượt trước.
   - Cả hai lỗi do review độc lập bắt được; các lượt đo đó đã bị loại.
4. **Phép kiểm "tiền đúng" bằng `SUM(postings)=0` không bắt được ghi sổ trùng** — bút toán trùng vẫn
   cân. Load test đã thêm kiểm số bút toán theo `transaction_id` và bảo toàn tổng tiền.
5. **Quá tải kéo dài sinh retry storm:** độ trễ vượt deadline saga 2 phút, job quét gửi lại hàng trăm
   lệnh đúng lúc hệ thống ngộp. Tiền vẫn đúng nhờ idempotency theo `transactionId`; còn hành vi thì
   **chưa sửa** — roadmap 4.5.

Các phát hiện trong lúc làm từng mục (đã sửa trong chính PR đó) được ghi ở ADR tương ứng. Ví dụ:
khử trùng theo event vỡ khi một event chạm hai ví (ADR-012); `REVERSAL` là cửa hậu rút tiền khỏi ví đóng
băng nếu không ràng buộc ảnh đối xứng (ADR-011).

---

## Theo PR

| PR | Mục roadmap | Nội dung chính | ADR |
|---|---|---|---|
| #17 | 1.0 | `tenant_id` trên mọi bảng ledger và mọi event; message thiếu tenant → DLT; hợp đồng message tách ra common-lib | nền cho ADR-010 |
| #18 | 1.0b | JWT claim `tid` → gateway xoá rồi gắn `X-Tenant-Id` → `TenantFilter`; vòng đời tài khoản sổ cái (`ACTIVE/FROZEN/CLOSED`), mở tài khoản qua command; `REVERSAL` chỉ hoàn tác đúng và một lần | ADR-010, ADR-011 |
| #20 | 1.1 + 1.3 | wallet-service: read model giao hoán (cộng delta + dedup theo ví), rebuild từ ledger, cursor pagination; JSON Schema cho mọi message + `EventContractTest` | ADR-012 |
| #22 | — | CI: tên image GHCR phải viết thường | — |
| #23 | 1.2a | transaction-service: nhận lệnh 202 idempotent theo `(tenant, user, key)`, replay khớp từng byte; RFC 7807 thống nhất; gateway chặn path đi vòng (`..`, `;`, `%2e`…) trước mọi so khớp | ADR-013 |
| #24 | 1.2b | Saga: máy trạng thái tường minh, ledger thắng, gửi lại bằng `eventId` mới; job quét saga treo → `MANUAL_REVIEW` + alert; event `txn.event.completed/failed.v1` | ADR-014 |
| #25 | 1.5a | E2E qua gateway trên stack thật + job CI `e2e`; sửa gateway không khởi động được | — |
| #26 | 1.5b | Load test k6 + số liệu cơ sở, kiểm tiền sau tải | — |

Cắt: **1.4 notification-service** — gộp vào webhook khách hàng (Phase 5.1), xem
[ADR-015](adr/ADR-015-product-positioning.md).

---

## Kiểm chứng

**153 test** chạy trên CI ở commit cuối của phase (log `Build & test` của #26), 0 fail, 0 skip. Lúc đóng
Phase 0 là 35 test.

| Module | Test |
|---|---|
| common-lib | 26 |
| api-gateway | 14 |
| auth-service | 7 |
| ledger-service | 42 |
| wallet-service | 27 |
| transaction-service | 37 |

Ngoài `mvn verify`, mọi PR từ #25 còn chạy **E2E trên stack compose thật** (`scripts/e2e/phase1-dod.sh`).

---

## Còn tồn đọng

Phase 1 không còn việc nào trong DoD. Mang sang:

- [ ] **Phase 0** — bật branch protection trên `master`. Nay nên bắt buộc cả check
  `E2E (compose + DoD Phase 1)`. Diễn tập restore đã làm (RTO 4 giây, 2026-10-16); nên lặp lại định kỳ.
- [ ] **Roadmap 4.5** — các việc tối ưu do load test chỉ ra: trần outbox relay, retry storm, ledger
  `concurrency`, cạn pool Hikari, JWKS 401, histogram.
- [ ] **Phase 2 — luồng rút tiền ra ngoài** (phần withdraw của 2.4, cùng 2.1 HOLD phục vụ nó) bị chặn tới
  khi có kết luận pháp lý: chỉ hoàn về nguồn đã nạp, hay rút tự do
  ([ADR-015](adr/ADR-015-product-positioning.md)). Phần nạp tiền không bị chặn.
