---
description: Đọc tiến độ roadmap hiện tại, chọn bước tiếp theo, đề xuất chiến lược và xin approve trước khi làm
argument-hint: [mục roadmap muốn xét, ví dụ "1.4" — bỏ trống để tự chọn theo đường găng]
allowed-tools: Bash(git status:*), Bash(git log:*), Bash(git diff:*), Bash(git branch:*), Bash(git rev-list:*), Bash(git fetch:*), Bash(gh pr list:*), Bash(gh pr view:*), Bash(gh pr checks:*), Bash(gh run list:*)
---
Mục được chỉ định: $ARGUMENTS (trống = tự chọn bước tiếp theo)

## Trạng thái lúc gọi lệnh
- Branch + thay đổi chưa commit: !`git status --short --branch`
- Commit gần nhất: !`git log --oneline -8`
- PR của tôi (mới nhất trước): !`gh pr list --state all --author "@me" --limit 10`

## Quy tắc
- Giai đoạn 1–4 là **CHỈ ĐỌC**: không sửa file, không tạo branch, không commit. Mọi thứ chỉ diễn ra sau khi tôi approve ở giai đoạn 5.
- Nguồn sự thật theo thứ tự ưu tiên: **code + PR đã merge** > ADR (`docs/adr/`) > roadmap (`docs/walletflow-production-roadmap.md`) > master plan. Roadmap có những đoạn "đề xuất ban đầu" đã bị ADR thay thế — đừng làm theo bản cũ.
- Không đoán quy tắc nghiệp vụ. Câu nào chưa chắc thì đưa vào mục "Câu hỏi cho bạn".
- Trả lời bằng tiếng Việt, ngắn gọn, có `path:line` khi dẫn code.

## Giai đoạn 1 — Đọc tiến độ hiện tại
1. Đọc bảng "Trạng thái Phase N" và mục "Definition of Done" của phase đang làm trong roadmap, kèm phần việc tồn đọng của các phase trước (ví dụ việc còn mở của Phase 0 trong `README.md`).
2. Đối chiếu bảng trạng thái với thực tế: PR nào đã MERGED mà roadmap vẫn ghi 🟡/⬜, branch nào đang dở dang. Nếu cần, dùng `gh pr view <n>` / `gh pr checks <n>`.
3. Kiểm tra working tree: đang ở branch nào, có thay đổi chưa commit, branch hiện tại đã merge chưa, có commit chưa push không (`git log origin/master..HEAD --oneline`), CI của PR đang mở xanh hay đỏ.

## Giai đoạn 2 — Chặn trước khi làm việc mới
Liệt kê những thứ phải xử lý TRƯỚC khi bắt đầu mục mới, nếu có:
- Thay đổi chưa commit, hoặc branch hiện tại chưa merge.
- PR đang mở có CI đỏ hoặc đang chờ review.
- Roadmap lệch với thực tế: đề xuất nội dung cập nhật bảng trạng thái, nhưng KHÔNG tự sửa.
- PR dependabot đang mở: chỉ đếm và nhắc, không phân tích từng cái.

## Giai đoạn 3 — Chọn bước tiếp theo
- Có `$ARGUMENTS`: phân tích đúng mục đó, nhưng vẫn cảnh báo nếu nó phụ thuộc mục chưa xong.
- Không có: chọn mục ⬜ đầu tiên trên **đường găng**, sau khi các phụ thuộc đã xong. Mục ghi "cắt được nếu thiếu thời gian" thì nêu phương án làm và phương án cắt. Nếu phase hiện tại đã đạt DoD, đề xuất đóng phase (đánh dấu DoD, tổng kết) trước khi sang phase sau.
- Nêu 1–2 phương án thay thế và lý do không chọn chúng.

## Giai đoạn 4 — Phân tích và chiến lược
1. Đọc section của mục đó trong roadmap, các ADR liên quan và runbook liên quan.
2. Dùng subagent `code-explorer` để lập bản đồ code sẽ bị chạm: service/module, topic Kafka trong `WalletTopics`, JSON Schema, migration, các `*IT` hiện có, những chỗ code thật đã khác với roadmap.
3. Trình bày theo khung dưới đây. Mỗi mục tối đa vài dòng; toàn bộ không quá khoảng 80 dòng.

```
### Hiện trạng
<phase, % DoD đã đạt, mục vừa xong, các điểm lệch roadmap ↔ thực tế>

### Cần xử lý trước (nếu có)
<danh sách ở giai đoạn 2 kèm cách xử lý>

### Bước tiếp theo: <mã mục> — <tên> (ước lượng roadmap: X ngày)
Lý do chọn: <phụ thuộc đã xong, nằm trên đường găng, giá trị cho DoD>
Phương án khác đã cân nhắc: <...>

### Phạm vi
- Làm: <...>
- Không làm (để sau / mục khác): <...>

### Chiến lược triển khai
- Branch: feat/phase-<n>-<slug> · PR: "Phase <n> (<k>/n): <tên> (<mã mục>)" — tách nhiều PR nếu > ~3 ngày công
- Hợp đồng trước: topic / JSON Schema / migration (service nào, V<next>)
- Các bước theo thứ tự (entity/repositories → services → listener/controller), mỗi bước đều compile được
- Bất biến tiền bị chạm → `*IT` chứng minh từng cái (ánh xạ tới DoD)
- Cần ADR mới? Runbook/alert mới? Cập nhật docker-compose / CI matrix / pom cha (module mới, failsafe, jacoco gate)?

### Rủi ro & cách giảm
<2–5 dòng>

### Kiểm chứng
<lệnh mvnw sẽ chạy + test kỳ vọng; review bằng java-reviewer trước commit>

### Câu hỏi cho bạn
<quyết định nghiệp vụ/phạm vi cần bạn chốt; không có thì ghi "Không">
```

## Giai đoạn 5 — Xin approve
Dùng tool AskUserQuestion, một câu hỏi duy nhất, với các lựa chọn:
- "Approve — bắt đầu thực hiện" (Recommended)
- "Approve nhưng xử lý phần 'Cần xử lý trước' trước"
- "Điều chỉnh chiến lược" (tôi sẽ ghi chỉnh sửa)
- "Chọn mục khác"

Nếu còn "Câu hỏi cho bạn", hỏi chúng trong cùng lần gọi (tối đa 3 câu, mỗi câu có lựa chọn đề xuất).
Sau đó **DỪNG**. Không làm gì thêm cho tới khi có câu trả lời.

## Sau khi được approve
- Tạo branch từ `master` mới nhất (`git checkout` sẽ hỏi quyền — đó là chủ ý).
- Thực hiện theo quy trình của `/feature` từ bước 3 (Contract first → Implement → Test → Review → Report), dùng kế hoạch đã duyệt làm bản kế hoạch. Không lập lại kế hoạch, trừ khi gặp điều trái với giả định; khi đó dừng lại báo tôi.
- Cập nhật bảng trạng thái roadmap (🟡 + tên branch) và ADR/runbook trong cùng PR.
- Kết thúc bằng báo cáo + commit message tiếng Việt đề xuất. Không push hay tạo PR nếu tôi chưa bảo; không bao giờ merge.
