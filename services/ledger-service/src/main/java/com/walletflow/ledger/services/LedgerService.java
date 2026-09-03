package com.walletflow.ledger.services;

import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.InsufficientFundsException;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.money.Money;
import com.walletflow.ledger.dto.JournalView;
import com.walletflow.ledger.dto.Leg;
import com.walletflow.ledger.dto.PostJournalCommand;
import com.walletflow.ledger.dto.ResolvedLeg;
import com.walletflow.ledger.entity.Account;
import com.walletflow.ledger.entity.JournalEntry;
import com.walletflow.ledger.entity.Posting;
import com.walletflow.ledger.enums.EJournalType;
import com.walletflow.ledger.enums.ETypeAccount;
import com.walletflow.ledger.repositories.AccountRepository;
import com.walletflow.ledger.repositories.JournalEntryRepository;
import com.walletflow.ledger.repositories.PostingRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Sổ cái double-entry — NGUỒN SỰ THẬT về tiền (plan §4.3).
 *
 * <p>Bảo đảm ba tính chất, mỗi cái ứng với một bài toán trong plan §1:
 * <ul>
 *   <li><b>Bảo toàn tiền</b>: mỗi bút toán có tổng các vế = 0, nên tổng toàn hệ thống luôn = 0 (P7).</li>
 *   <li><b>Idempotent theo {@code (transactionId, type)}</b>: gửi lại lệnh trả về đúng bút toán cũ,
 *       không ghi thêm (P2).</li>
 *   <li><b>Không bao giờ âm</b>: kiểm tra số dư và ghi bút toán nằm trong cùng một vùng khoá (P3, P6).</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LedgerService {

    private final AccountRepository accountRepository;
    private final JournalEntryRepository journalEntryRepository;
    private final PostingRepository postingRepository;

    /**
     * Ghi bút toán, hoặc trả về bút toán đã ghi nếu lệnh này đã được xử lý trước đó.
     *
     * <p><b>Ngữ nghĩa idempotent (plan §7.2):</b> lệnh trùng KHÔNG phải lỗi, mà là chuyện bình thường
     * — Kafka giao at-least-once nên cùng một {@code PostJournalCommand} có thể đến nhiều lần. Vì vậy
     * hàm này TRẢ VỀ KẾT QUẢ CŨ chứ không ném lỗi trùng. Đây không phải chi tiết vụn: nếu ledger báo
     * lỗi khi nhận lại lệnh, Saga orchestrator rất dễ hiểu đó là "ghi sổ thất bại" và chạy bù trừ
     * (hoàn tiền) cho một giao dịch mà tiền ĐÃ vào sổ — tức là mất tiền thật.
     *
     * <p>Bút toán trùng được phát hiện ở hai chỗ, cả hai đều cần:
     * <ol>
     *   <li><b>Trước khi khoá</b> — đường tắt cho trường hợp phổ biến nhất: lệnh được giao lại sau
     *       khi lần trước đã hoàn tất.</li>
     *   <li><b>Sau khi đã khoá tài khoản</b> — nếu hai lệnh trùng chạy song song, lệnh thứ hai vừa
     *       xếp hàng chờ khoá; đến lượt nó thì lệnh thứ nhất đã commit, nên lúc này (và chỉ lúc này)
     *       nó mới nhìn thấy bút toán kia. Không có bước này, lệnh thứ hai sẽ đi tiếp và vỡ ở ràng
     *       buộc UNIQUE.</li>
     * </ol>
     * Ràng buộc {@code uk_journal_txn_type} vẫn là chốt chặn cuối: nếu nó vẫn vỡ thì đó là lỗi TẠM
     * THỜI (hai lệnh trùng trên hai instance, chạm tập tài khoản khác nhau) — cố tình để exception
     * nổ ra cho tầng trên retry, chứ không biến thành "từ chối nghiệp vụ".
     */
    @Transactional
    public JournalEntry postJournal(PostJournalCommand command) {

        validateShape(command);
        validateReference(command);

        Optional<JournalEntry> alreadyPosted = findExisting(command);
        if (alreadyPosted.isPresent()) {
            log.info("Ledger: lệnh trùng cho transaction {} ({}) -> trả về bút toán đã ghi {}",
                    command.transactionId(), command.type(), alreadyPosted.get().getId());
            return alreadyPosted.get();
        }

        List<ResolvedLeg> resolvedLegs = resolveAccounts(command);

        Optional<JournalEntry> postedWhileWaiting = findExisting(command);
        if (postedWhileWaiting.isPresent()) {
            log.info("Ledger: một lệnh trùng đã commit trong lúc chờ khoá cho transaction {} ({})",
                    command.transactionId(), command.type());
            return postedWhileWaiting.get();
        }

        validateSufficientFunds(resolvedLegs, command);

        JournalEntry entry = journalEntryRepository.saveAndFlush(
                JournalEntry.builder()
                        .transactionId(command.transactionId())
                        .type(command.type())
                        .referenceId(command.referenceId())
                        .build());

        postingRepository.saveAll(createPostings(entry, resolvedLegs));

        return entry;
    }

    /**
     * Đọc lại một bút toán từ sổ để dựng event — xem {@link JournalView} về lý do không dùng lại
     * {@code command.legs()}.
     */
    @Transactional(readOnly = true)
    public JournalView describe(JournalEntry entry) {
        List<Leg> legs = new ArrayList<>();
        String debitedWallet = null;
        String anyWallet = null;

        for (Posting posting : postingRepository.findByEntryId(entry.getId())) {
            Account account = accountRepository.findById(posting.getAccountId())
                    .orElseThrow(() -> new ResourceNotFoundException(
                            "Account not found: " + posting.getAccountId()));

            legs.add(new Leg(account.getExternalRef(), posting.getAmountMinor()));

            if (account.getType() != ETypeAccount.USER) {
                continue;
            }
            if (anyWallet == null) {
                anyWallet = account.getExternalRef();
            }
            if (posting.getAmountMinor() < 0 && debitedWallet == null) {
                debitedWallet = account.getExternalRef();
            }
        }

        return new JournalView(legs, debitedWallet != null ? debitedWallet : anyWallet);
    }

    private Optional<JournalEntry> findExisting(PostJournalCommand command) {
        return journalEntryRepository
                .findByTransactionIdAndType(command.transactionId(), command.type());
    }

    /**
     * Kiểm tra bút toán HỢP LỆ VỀ HÌNH DẠNG trước khi ghi bất cứ thứ gì (plan §5.1).
     *
     * <p>Ba điều kiện, mỗi điều kiện chặn một cách phá vỡ bất biến bảo toàn tiền:
     * <ol>
     *   <li><b>≥ 2 vế:</b> một chuyển động tiền luôn có nơi đi và nơi đến. Không chặn thì
     *       {@code legs = []} có SUM = 0 nên "cân bằng" một cách vô nghĩa, và ta sẽ ghi một
     *       journal entry KHÔNG có posting nào.</li>
     *   <li><b>Không vế nào bằng 0:</b> vế 0 không mang thông tin. Ràng buộc
     *       {@code CHECK (amount_minor <> 0)} ở DB cũng chặn, nhưng chỉ nổ lúc flush và biến
     *       thành lỗi 500; chặn ở đây để trả về 400 rõ ràng. Đồng thời (cùng với SUM = 0 và
     *       ≥ 2 vế) nó bảo đảm luôn có ít nhất một vế nợ VÀ một vế có.</li>
     *   <li><b>SUM = 0 tính qua {@link Money}:</b> phép cộng của Money dùng {@code Math.addExact} nên
     *       tràn số NỔ RA thay vì âm thầm wrap — {@code [MAX_VALUE, MAX_VALUE, 2]} cộng bằng toán tử
     *       {@code +} thông thường sẽ ra đúng 0 và lọt qua kiểm tra "cân bằng" trong khi thực chất là
     *       tiền tự sinh. Đây cũng là lý do ADR-005 cấm dùng {@code +}/{@code -} trần trên tiền.</li>
     * </ol>
     */
    private void validateShape(PostJournalCommand command) {
        List<Leg> legs = command.legs();

        if (legs == null || legs.size() < 2) {
            throw new BadRequestException(
                    "Journal must have at least 2 legs, got " + (legs == null ? 0 : legs.size()));
        }

        Money sum = Money.zero(command.currency().name());
        try {
            for (Leg leg : legs) {
                if (leg.amountMinor() == 0) {
                    throw new BadRequestException(
                            "Journal leg amount must not be 0: " + leg.accountRef());
                }
                sum = sum.plus(Money.of(leg.amountMinor(), command.currency().name()));
            }
        } catch (ArithmeticException overflow) {
            throw new BadRequestException("AMOUNT_OVERFLOW",
                    "Tổng các vế vượt quá miền giá trị long");
        }

        if (!sum.isZero()) {
            throw new BadRequestException("Journal is not balanced. SUM = " + sum.amountMinor());
        }
    }

    /**
     * {@code referenceId} chỉ có nghĩa với bút toán bù trừ, và với bút toán bù trừ thì nó BẮT BUỘC
     * (plan §5.2, §7.4).
     *
     * <p>Một {@code REVERSAL} không biết mình đang bù cho bút toán nào là bút toán trôi nổi: không
     * đối soát được, không trả lời được câu hỏi "giao dịch này đã được hoàn chưa" — mà khả năng trả
     * lời những câu hỏi đó chính là lý do sổ cái tồn tại. Ngược lại, {@code referenceId} trên một bút
     * toán thường là dấu hiệu người gọi đang hiểu sai ngữ nghĩa, nên cũng từ chối luôn thay vì lặng
     * lẽ lưu một trường vô nghĩa.
     */
    private void validateReference(PostJournalCommand command) {
        boolean isReversal = command.type() == EJournalType.REVERSAL;

        if (isReversal) {
            if (command.referenceId() == null) {
                throw new BadRequestException("REVERSAL requires referenceId trỏ về bút toán gốc");
            }
            if (!journalEntryRepository.existsById(command.referenceId())) {
                throw new ResourceNotFoundException(
                        "Journal gốc không tồn tại: " + command.referenceId());
            }
        } else if (command.referenceId() != null) {
            throw new BadRequestException(
                    "referenceId chỉ dùng cho REVERSAL, không dùng cho " + command.type());
        }
    }

    /**
     * Nạp các tài khoản của bút toán và KHOÁ HÀNG chúng ({@code SELECT ... FOR UPDATE}).
     *
     * <p>Vì số dư ở đây là giá trị suy ra ({@code SUM(postings)}) chứ không phải một cột, không có
     * hàng nào để gắn {@code @Version} và cũng không viết được {@code UPDATE ... WHERE balance >= x}.
     * Nên ta dùng chính hàng {@code accounts} làm "mutex" cho tài khoản đó: giữa lúc đọc số dư và
     * lúc ghi posting, không transaction nào khác chen vào được (giải P3/P6).
     *
     * <p>Khoá được lấy theo thứ tự {@code accountRef} đã sắp xếp, nên mọi transaction đều xin khoá
     * theo CÙNG một trình tự — điều kiện cần để không bao giờ deadlock chéo.
     *
     * <p>Đánh đổi đã biết: mọi top-up/withdraw đều khoá {@code EXTERNAL_BANK} nên bị tuần tự hoá
     * với nhau. Transfer nội bộ (ví→ví) không chạm tài khoản hệ thống nên vẫn song song tốt.
     */
    private List<ResolvedLeg> resolveAccounts(PostJournalCommand command) {

        List<Leg> orderedLegs = command.legs().stream()
                .sorted(Comparator.comparing(Leg::accountRef))
                .toList();

        List<ResolvedLeg> resolvedLegs = new ArrayList<>();

        for (Leg leg : orderedLegs) {
            Account account = accountRepository
                    .findForUpdate(leg.accountRef(), command.currency())
                    .orElseThrow(() -> new ResourceNotFoundException(
                            "Account not found: " + leg.accountRef()));

            resolvedLegs.add(new ResolvedLeg(leg, account));
        }

        return resolvedLegs;
    }

    /**
     * Chặn số dư âm cho ví người dùng (P6).
     *
     * <p>Chỉ kiểm tra tài khoản {@code USER}: tài khoản hệ thống như {@code EXTERNAL_BANK} PHẢI được
     * âm — số dư âm của nó chính là lượng tiền đã đi vào hệ thống từ thế giới ngoài (plan §5.1).
     * Ràng buộc "không âm" áp cho tài khoản hệ thống sẽ khiến mọi lệnh nạp tiền thất bại.
     */
    private void validateSufficientFunds(List<ResolvedLeg> resolvedLegs, PostJournalCommand command) {

        String currency = command.currency().name();
        Map<UUID, Money> debitByAccount = new HashMap<>();

        for (ResolvedLeg resolvedLeg : resolvedLegs) {
            Account account = resolvedLeg.account();
            long amount = resolvedLeg.leg().amountMinor();

            if (!isUserDebit(account, amount)) {
                continue;
            }

            // Gộp theo tài khoản: một bút toán có thể ghi nợ cùng một ví ở nhiều vế
            debitByAccount.merge(
                    account.getId(),
                    Money.of(amount, currency).negate(),
                    Money::plus);
        }

        for (Map.Entry<UUID, Money> debit : debitByAccount.entrySet()) {
            Money required = debit.getValue();
            Money balance = Money.of(postingRepository.balanceOf(debit.getKey()), currency);

            if (!balance.isGreaterThanOrEqual(required)) {
                throw new InsufficientFundsException(
                        "Insufficient funds. Balance = " + balance.amountMinor()
                                + ", required = " + required.amountMinor());
            }
        }
    }

    private boolean isUserDebit(Account account, long amount) {
        return ETypeAccount.USER.equals(account.getType()) && amount < 0;
    }

    private List<Posting> createPostings(JournalEntry entry, List<ResolvedLeg> resolvedLegs) {
        return resolvedLegs.stream()
                .map(resolvedLeg -> Posting.builder()
                        .entryId(entry.getId())
                        .accountId(resolvedLeg.account().getId())
                        .amountMinor(resolvedLeg.leg().amountMinor())
                        .build())
                .toList();
    }
}
