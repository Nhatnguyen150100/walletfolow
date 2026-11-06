# Runbook — đi hết một lần nạp tiền trên sandbox VNPay (DoD Phase 2, ô 1)

> Không phải runbook của một alert. Đây là **thủ tục lấy bằng chứng** cho ô Definition of Done
> "nạp tiền thật qua sandbox VNPay/Stripe → số dư tăng đúng"
> ([roadmap §Phase 2](../walletflow-production-roadmap.md)).
>
> Đường nạp đã có `VnPayChargeIT`, nhưng nó chạy với `FakeVnPayServer` **trong cùng tiến trình**.
> Chưa lần nào `transaction-service → Kafka → bank-adapter → VNPay thật → querydr → ledger → read
> model` được đi hết trên một stack thật. Ô DoD này đóng bằng một lần chạy tay, không bằng một test.

## Vì sao không cần tunnel ra internet

Câu hỏi đầu tiên ai cũng hỏi: IPN của VNPay làm sao tới được `localhost`?

**Không cần.** Kết quả cuối của một lần thu chỉ được phát sau khi bank-adapter **tự hỏi lại** VNPay
bằng `querydr` — webhook chỉ là gợi ý để hỏi sớm hơn
([ADR-016](../adr/ADR-016-bank-adapter-charge-model.md) §1). `PendingChargeReconciler`
(`services/bank-adapter/src/main/java/com/walletflow/bank/services/PendingChargeReconciler.java:106`)
tự hỏi lại, nên vòng nạp khép lại mà không có IPN nào.

**Nhưng nó không hỏi liên tục.** 10 giây chỉ là nhịp *quét lô*; mỗi lần thu chỉ được hỏi lại khi tới
hạn `next_check_at`, và khoảng đó giãn theo cấp số — 30s, 1m, 2m, 4m, 8m, rồi mỗi 10 phút
(`status-check.recheck-after` → `max-recheck-after`, ADR-017). Hệ quả thực tế: trả tiền ở phút 14 thì
có thể tới phút 24 mới được xác nhận. Đó là lý do `PAY_TIMEOUT` mặc định là **2400 giây**, không phải
15 phút — chờ ngắn hơn sẽ sinh ra một file bằng chứng `failed` cho một lần nạp đã thành công.

Return URL thì là **redirect phía trình duyệt**, không phải lời gọi server-to-server — trình duyệt
trên máy bạn mở `http://localhost:8080/payments/return/vnpay` được. Trang đó cũng chỉ để hiển thị:
tham số trên URL không bao giờ là kết quả (`PaymentReturnIT`).

Muốn chứng minh luôn cả đường IPN thì tự dựng tunnel (cloudflared/ngrok), trỏ `VNPAY_RETURN_URL` và
IPN URL trong portal merchant ra đó. Thủ tục dưới đây không đòi, và **không nên** để lần lấy bằng
chứng đầu tiên phụ thuộc thêm một thành phần nữa.

## Chuẩn bị

1. `.env` có đủ sáu biến, lấy từ portal merchant sandbox của bạn — repo **không** có mặc định nào
   (CLAUDE.md §6, service sẽ không khởi động nếu thiếu):
   `VNPAY_TMN_CODE`, `VNPAY_HASH_SECRET`, `VNPAY_PAY_URL`, `VNPAY_API_URL`, `VNPAY_RETURN_URL`,
   `VNPAY_SERVER_IP`.
2. `VNPAY_RETURN_URL = http://localhost:8080/payments/return/vnpay` — tên provider trên đường dẫn
   phải khớp provider đang chạy (`PaymentReturnService:49`).
3. Docker Desktop đang chạy, và **không có stack walletflow nào khác đang bật**: tên container là
   `wf-*` cố định trong `docker-compose.yml`, hai stack không sống chung được.
4. Thẻ test của VNPay sandbox (ngân hàng NCB trong bộ thẻ demo của họ).

## Chạy

```bash
set -a && source .env && set +a
export BANK_PROVIDER=vnpay        # PHẢI export trước up.sh — xem cái bẫy bên dưới
./scripts/e2e/up.sh
./scripts/e2e/topup-vnpay.sh
```

> **Cái bẫy:** `ensure_bank_provider` (`scripts/e2e/lib.sh:145`) đặt `BANK_PROVIDER=mock` nếu shell
> chưa có biến đó, và **biến của shell thắng `.env`** khi compose nội suy. Quên `export` thì stack
> lặng lẽ chạy với provider giả, mọi thứ vẫn xanh, và bằng chứng vô giá trị. Kịch bản in rõ provider
> nó đang dùng ở đầu và cuối — đọc dòng đó.

Kịch bản dừng sau bước 4 và in trang thanh toán. Mở trên trình duyệt, trả bằng thẻ test. Trong lúc đó
nó poll tới khi giao dịch về một trạng thái kết thúc — tối đa `PAY_TIMEOUT`, mặc định **2400 giây** =
hạn phiên `PT15M` + `expiry-grace` `PT15M` + một nhịp hỏi lại `PT10M`, vẫn nằm trong
`topup.payment-deadline` `PT45M` của saga.

Access token chỉ sống 900 giây (`auth-service: access-ttl-seconds`), ngắn hơn khoảng chờ đó — kịch
bản tự đăng nhập lại khi gặp `401`, bạn không phải làm gì.

Biến đổi được: `AMOUNT_MINOR` (mặc định 10000 = 10.000 đ), `PAY_TIMEOUT`, `EVIDENCE_DIR`.

### Thử kịch bản mà không tiêu tiền

```bash
export BANK_PROVIDER=mock BANK_MOCK_ALLOWED=true
./scripts/e2e/up.sh && ./scripts/e2e/topup-vnpay.sh
```

`MockProvider` tự chốt phiên sau ~10 giây (`walletflow.bank.mock.auto-settle-after`) nên chạy hết
không cần trình duyệt. Dùng để kiểm chính kịch bản; file bằng chứng sinh ra có cảnh báo ở đầu và
**không** đóng được ô DoD.

## Kịch bản kiểm những gì

| Bước | Khẳng định |
|---|---|
| 1 | `POST /api/v1/transactions/topup` trả `202` — **API thật**, không phải cửa sau Kafka như `lib.sh:topup` |
| 2 | `PENDING → AWAITING_PAYMENT` và có `paymentUrl` ([ADR-019](../adr/ADR-019-topup-saga.md)) |
| 3 | Gửi lại cùng `Idempotency-Key` lúc lệnh **còn dang dở**: `Idempotent-Replayed: true`, body khớp từng byte (bất biến §2) |
| 4 | **Chưa trả tiền thì chưa ghi gì**: sổ cái ví vẫn 0, chưa có bút toán nào, read model vẫn 0 |
| 5 | Giao dịch về một trạng thái kết thúc, và trạng thái đó là `COMPLETED` |
| 6 | Sổ cái ví tăng đúng số đã nạp · read model khớp · đúng **một** bút toán · `SUM(postings) = 0` theo tenant (bất biến §1) |
| 7 | `provider_transactions`: đúng **một** lần thu cho ví, `SUCCEEDED`, số tiền khớp, có `raw_response`, `provider_ref` khớp cái API trả cho người dùng |
| 7 | `raw_request`/`raw_response` **không** chứa `VNPAY_HASH_SECRET` ([ADR-017](../adr/ADR-017-vnpay-session-expiry-log-masking.md)) — chỉ chạy ở chế độ `vnpay` |
| cuối | Không message nào rơi vào DLT trong suốt lần chạy |

Hai bước đặt đúng chỗ, không phải cho gọn:

- **Bước 3 nằm trước lúc trả tiền.** Một lần gửi lại do timeout xảy ra đúng vào lúc lệnh còn dang dở;
  kiểm nó sau khi mọi thứ đã xong là kiểm vào lúc dễ nhất.
- **Bước 4 chỉ kiểm được ở đúng khoảnh khắc đó** — lệnh đã nhận, phiên đã mở, tiền chưa trả. Nếu ví
  đã tăng ở đây thì hệ thống đang cộng tiền dựa trên một lời hứa chứ không phải một lần thu, và mọi
  con số ở bước 6 vẫn sẽ đúng y như cũ.

## Bằng chứng

Kịch bản ghi `docs/evidence/phase-2-topup-vnpay/<thời điểm UTC>-<provider>-<kết cục>.md`, **kể cả khi
hỏng** — một lần nạp thất bại trên sandbox thật đáng lưu ngang một lần thành công. (Hỏng *trước* lúc
lệnh nạp được nhận — đăng ký, mở ví — thì chưa có gì để ghi và không có file nào.)

Bạn đính kèm thêm vào cùng thư mục:

- Ảnh chụp trang thanh toán VNPay và trang quay về.
- `docker compose -p wf-e2e logs bank-adapter transaction-service > ...log` của khoảng thời gian đó.

**Trước khi commit, rà lại:** `vnp_TmnCode`, `vnp_SecureHash`, số thẻ test không được có trong file
nào. Kịch bản tự cắt query string của mọi URL nó *ghi vào file bằng chứng*, nhưng ba thứ sau thì bạn
tự chịu trách nhiệm:

- **Ảnh chụp màn hình** — thanh địa chỉ của trình duyệt mang đủ cả `vnp_TmnCode` lẫn `vnp_SecureHash`.
- **Log container.**
- **Output của chính kịch bản.** Nó *phải* in nguyên trang thanh toán ra màn hình để bạn mở được. Một
  lần `./scripts/e2e/topup-vnpay.sh 2>&1 | tee run.log` rồi để `run.log` cạnh file bằng chứng là đủ
  để bí mật merchant vào repo.

Chạy xong mới tick ô DoD trong roadmap, kèm đường dẫn tới file bằng chứng. **Không tick trước.**

## Khi nó hỏng

Kịch bản dừng ngay ở bước hỏng với exit code ≠ 0.

| Triệu chứng | Nơi xem |
|---|---|
| Giao dịch về `MANUAL_REVIEW` hoặc `FAILED` | [`topup-stuck.md`](topup-stuck.md) — kịch bản in sẵn `failureCode` |
| Đứng mãi ở `AWAITING_PAYMENT` sau khi đã trả tiền | `docker compose -p wf-e2e logs bank-adapter`, tìm `querydr`. VNPay sandbox trả lời chậm là chuyện thường |
| Không tạo nổi phiên (không bao giờ tới `AWAITING_PAYMENT`) | Sai `VNPAY_TMN_CODE`/`VNPAY_HASH_SECRET`, hoặc `VNPAY_SERVER_IP` không nằm trong allowlist của merchant |
| Circuit breaker mở | [`payment-provider-down.md`](payment-provider-down.md) |
| Hết `PAY_TIMEOUT` | Quá hạn phiên 15 phút thì VNPay không nhận tiền nữa — chạy lại từ đầu, lệnh cũ sẽ tự về `FAILED(EXPIRED)` |

Dọn sạch: `./scripts/e2e/down.sh` — **xoá cả volume** (`down -v`), đừng chạy khi còn dữ liệu cần giữ.
