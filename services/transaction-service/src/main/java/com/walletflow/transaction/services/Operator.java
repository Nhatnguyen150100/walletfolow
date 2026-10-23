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

    /**
     * @throws UnauthorizedException gateway không gắn định danh — gần như luôn là gọi thẳng vào
     *                               service, bỏ qua biên
     * @throws ForbiddenException    biết là ai, nhưng vai không được vào khu vực vận hành
     */
    public static Operator requireReader(UUID id, String email, String role, String ip) {
        if (id == null) {
            throw new UnauthorizedException("Thiếu định danh người dùng");
        }
        if (role == null || !MAY_READ.contains(role)) {
            throw new ForbiddenException("Tài khoản không có quyền vào khu vực quản trị");
        }
        return new Operator(id, email, role, ip);
    }
}
