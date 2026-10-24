package com.walletflow.transaction.services;

import com.walletflow.common.exception.ForbiddenException;
import com.walletflow.common.exception.UnauthorizedException;

import java.util.Set;
import java.util.UUID;

/**
 * Người vận hành đang thực hiện một thao tác, dựng từ header gateway gắn vào (roadmap 2.5,
 * ADR-021). Mang theo cả vai để mọi bản ghi audit biết người đó đang là ai LÚC ĐÓ.
 *
 * <p>Gateway đã chặn thô khu vực {@code /api/v1/admin/**} cho ba vai này, nhưng service vẫn kiểm
 * lại: biên trong cụm chưa có mTLS (Phase 4), và một lỗi cấu hình route không được phép là thứ duy
 * nhất đứng giữa khách hàng với dữ liệu của người khác.
 *
 * @param role vai thô từ header — giữ nguyên chuỗi thay vì map sang enum, vì {@code ERole} thuộc
 *             auth-service và service này không phụ thuộc vào đó
 */
public record Operator(UUID id, String email, String role, String ip) {

    /** Vai được XEM khu vực vận hành. Phải khớp {@code ERole} của auth-service (auth V3). */
    private static final Set<String> MAY_READ = Set.of("ADMIN", "OPS", "AUDITOR");

    /** Vai được ĐỀ XUẤT một thao tác xử lý ca. {@code AUDITOR} chỉ đọc nên không có mặt. */
    private static final Set<String> MAY_PROPOSE = Set.of("ADMIN", "OPS");

    /**
     * Vai được DUYỆT thao tác đụng tiền. Chỉ {@code ADMIN} — đây là nửa thứ hai của 4-eyes, và
     * {@code OPS} cố ý không có: nó mở được ca, nhưng không tự quyết chuyện tiền (ERole, auth V3).
     */
    private static final Set<String> MAY_APPROVE = Set.of("ADMIN");

    /**
     * @throws UnauthorizedException gateway không gắn định danh — gần như luôn là gọi thẳng vào
     *                               service, bỏ qua biên
     * @throws ForbiddenException    biết là ai, nhưng vai không được vào khu vực vận hành
     */
    public static Operator requireReader(UUID id, String email, String role, String ip) {
        return require(id, email, role, ip, MAY_READ, "Tài khoản không có quyền vào khu vực quản trị");
    }

    /**
     * Người ĐỀ XUẤT một thao tác xử lý ca. Gateway chặn theo tiền tố đường dẫn chứ KHÔNG theo
     * method, nên một {@code AUDITOR} vẫn gửi được POST tới đây — chốt chặn là chỗ này.
     */
    public static Operator requireProposer(UUID id, String email, String role, String ip) {
        return require(id, email, role, ip, MAY_PROPOSE,
                "Vai này chỉ được đọc, không đề xuất được thao tác xử lý");
    }

    /** Người DUYỆT — nửa thứ hai của 4-eyes. Còn một điều kiện nữa: không được là người đề xuất. */
    public static Operator requireApprover(UUID id, String email, String role, String ip) {
        return require(id, email, role, ip, MAY_APPROVE,
                "Chỉ ADMIN mới duyệt được thao tác đụng tiền");
    }

    private static Operator require(UUID id, String email, String role, String ip,
                                    Set<String> allowed, String denied) {
        if (id == null) {
            throw new UnauthorizedException("Thiếu định danh người dùng");
        }
        if (role == null || !allowed.contains(role)) {
            throw new ForbiddenException(denied);
        }
        return new Operator(id, email, role, ip);
    }
}
