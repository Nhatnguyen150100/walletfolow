package com.walletflow.ledger.services;

import com.walletflow.common.event.OpenAccountCommand;
import com.walletflow.common.exception.BadRequestException;
import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.ledger.AccountBalance;
import com.walletflow.common.ledger.AccountBalancesRequest;
import com.walletflow.common.ledger.AccountStatement;
import com.walletflow.common.ledger.StatementLine;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.ledger.dto.StatementRow;
import com.walletflow.ledger.entity.Account;
import com.walletflow.ledger.enums.ETypeAccount;
import com.walletflow.ledger.repositories.AccountRepository;
import com.walletflow.ledger.repositories.PostingRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Vòng đời tài khoản sổ cái (roadmap 1.0b, ADR-011).
 *
 * <p>Đây là con đường DUY NHẤT để một tài khoản người dùng ra đời. {@link LedgerService} không bao
 * giờ tự tạo tài khoản khi gặp một ref lạ — một lệnh chuyển tới ví gõ sai mã phải bị từ chối, chứ
 * không được "thành công" vào một tài khoản ma.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private final PostingRepository postingRepository;

    /**
     * Mở tài khoản {@code USER} cho một ví — idempotent: gọi lại trả về đúng tài khoản đã có, với
     * trạng thái HIỆN TẠI của nó.
     *
     * @throws BadRequestException nếu ref không phải UUID chuẩn, hoặc trùng một tài khoản không
     *                             phải {@code USER}
     */
    @Transactional
    public Account openUserAccount(OpenAccountCommand command) {
        UUID tenantId = TenantContext.require();
        String ref = requireCanonicalUuid(command.accountRef());
        ECurrency currency = command.currency();
        if (currency == null) {
            throw new BadRequestException("Thiếu currency cho tài khoản " + ref);
        }

        int created = accountRepository.insertIfAbsent(
                tenantId, ref, ETypeAccount.USER.name(), currency.name());

        Account account = accountRepository
                .findByTenantIdAndExternalRefAndCurrency(tenantId, ref, currency)
                .orElseThrow(() -> new IllegalStateException(
                        "Không đọc lại được tài khoản vừa tạo/đã có: " + ref));

        // Ref đã thuộc về một tài khoản hệ thống. Trả nó về như "mở thành công" nghĩa là một ví
        // người dùng được trỏ thẳng vào EXTERNAL_BANK — mọi kiểm tra số dư âm bị bỏ qua (tài khoản
        // hệ thống được phép âm). Với ref là UUID thì gần như không thể xảy ra; kiểm tra vẫn rẻ.
        if (account.getType() != ETypeAccount.USER) {
            throw new BadRequestException("ACCOUNT_REF_RESERVED",
                    "Ref " + ref + " thuộc về một tài khoản hệ thống");
        }

        log.info("Ledger: {} tài khoản {} ({}) cho tenant {} — trạng thái {}",
                created == 1 ? "mở mới" : "đã có", ref, currency, tenantId, account.getStatus());
        return account;
    }

    /**
     * Sao kê đầy đủ của một tài khoản — nguồn để dựng lại read model (roadmap 1.1).
     *
     * <p>{@code REPEATABLE_READ}: tài khoản và các dòng là HAI câu truy vấn. Ở mức mặc định (READ
     * COMMITTED) mỗi câu có snapshot riêng, nên trạng thái có thể thuộc một thời điểm khác danh sách
     * dòng. Ở đây cả hai cùng một snapshot — sao kê là ảnh chụp của sổ tại MỘT thời điểm.
     */
    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public AccountStatement statementOf(String ref, ECurrency currency) {
        UUID tenantId = TenantContext.require();
        Account account = accountRepository
                .findByTenantIdAndExternalRefAndCurrency(tenantId, ref, currency)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Không có tài khoản " + ref + " (" + currency + ")"));

        List<StatementLine> lines = postingRepository.statementOf(tenantId, account.getId()).stream()
                .map(StatementRow::toLine)
                .toList();

        return new AccountStatement(account.getId(), account.getExternalRef(), account.getCurrency(),
                account.getStatus(), account.getStatusVersion(), lines);
    }

    /**
     * Số dư của NHIỀU tài khoản cùng loại tiền — phục vụ đối soát tầng 2 (roadmap 3.1, ADR-023 §3).
     *
     * <p>Ref không có tài khoản thì KHÔNG có mặt trong kết quả, và đó là một câu trả lời có nghĩa
     * chứ không phải lỗi: bên đối soát đọc nó thành "ledger chưa có tài khoản cho ví này". Vì vậy
     * không ném {@code ResourceNotFoundException} như {@link #statementOf} — một ví chưa mở xong
     * trong một lô 500 ví không được làm hỏng cả lô.
     *
     * <p>{@code REPEATABLE_READ} như {@link #statementOf}: cả lô phải là ảnh chụp của sổ tại MỘT
     * thời điểm, nếu không thì hai ví trong cùng một lô được so với hai trạng thái sổ khác nhau.
     */
    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public List<AccountBalance> balancesOf(AccountBalancesRequest request) {
        UUID tenantId = TenantContext.require();
        if (request == null || request.currency() == null) {
            throw new BadRequestException("Thiếu currency");
        }
        List<String> refs = request.refs();
        if (refs == null || refs.isEmpty()) {
            throw new BadRequestException("Phải có ít nhất một accountRef");
        }
        if (refs.size() > AccountBalancesRequest.MAX_REFS) {
            throw new BadRequestException("TOO_MANY_ACCOUNT_REFS",
                    "Tối đa " + AccountBalancesRequest.MAX_REFS + " accountRef mỗi lượt, nhận "
                            + refs.size());
        }

        return postingRepository.balancesOf(tenantId, request.currency().name(), refs).stream()
                .map(row -> new AccountBalance(row.getAccountRef(), row.getBalanceMinor()))
                .toList();
    }

    /**
     * Ref phải là UUID ở dạng chuẩn chữ thường.
     *
     * <p>Không chỉ là "parse được thành UUID": {@code UUID.fromString} chấp nhận cả {@code
     * "A1B2..."} lẫn {@code "a1b2..."}, nhưng unique key so sánh CHUỖI. Hai cách viết của cùng một
     * wallet id sẽ thành hai tài khoản khác nhau — số dư của một ví bị chẻ đôi trong sổ cái.
     */
    private static String requireCanonicalUuid(String ref) {
        if (ref == null) {
            throw new BadRequestException("Thiếu accountRef");
        }
        try {
            String canonical = UUID.fromString(ref).toString();
            if (canonical.equals(ref)) {
                return ref;
            }
        } catch (IllegalArgumentException ignored) {
            // rơi xuống dưới
        }
        throw new BadRequestException("INVALID_ACCOUNT_REF",
                "accountRef phải là UUID dạng chuẩn chữ thường, nhận: " + ref);
    }
}
