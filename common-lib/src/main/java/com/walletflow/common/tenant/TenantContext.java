package com.walletflow.common.tenant;

import org.slf4j.MDC;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Tenant đang được phục vụ trong luồng hiện tại.
 *
 * <p><b>Đây là một biên giới bảo mật, không phải một tiện ích truyền tham số.</b> Mỗi truy vấn chạm
 * dữ liệu nghiệp vụ đều phải lọc theo tenant; rò rỉ chéo tenant trong một hệ thống giữ tiền không
 * phải là bug hiển thị mà là sự cố nghiêm trọng. Vì vậy class này được thiết kế theo nguyên tắc
 * <b>mặc định là từ chối</b>:
 *
 * <ul>
 *   <li>{@link #require()} <b>ném lỗi</b> khi chưa có tenant, thay vì lặng lẽ trả về một giá trị
 *       mặc định. Một default ngầm là cách rò rỉ chéo tenant phổ biến nhất: code chạy đúng trong
 *       mọi bài test một tenant, rồi ghi dữ liệu của khách A vào tenant mặc định ở production.</li>
 *   <li>Tenant chỉ được đặt ở <b>biên vào</b> — filter HTTP đọc header do gateway gắn, hoặc Kafka
 *       listener đọc {@code envelope.tenantId}. Không tầng nghiệp vụ nào được tự đặt.</li>
 *   <li>Luôn dùng {@link #callWith} / {@link #runWith} thay vì {@code set}+{@code clear} thủ công:
 *       thread trong pool được tái sử dụng, nên quên {@code clear()} nghĩa là request kế tiếp trên
 *       cùng thread thừa hưởng tenant của request trước.</li>
 * </ul>
 *
 * <p>Giá trị cũng được đẩy vào MDC để mọi dòng log mang theo tenant — khi điều tra sự cố, câu hỏi
 * đầu tiên luôn là "của khách nào".
 */
public final class TenantContext {

  /** Khoá MDC; trùng tên với field trong {@code EventEnvelope} cho dễ lần theo. */
  public static final String MDC_KEY = "tenantId";

  /**
   * Tenant mặc định cho giai đoạn hệ thống mới có một người dùng duy nhất (chính mình).
   *
   * <p>Cố tình là một UUID cố định chứ không phải {@code null}: dữ liệu đã có từ trước vẫn thuộc về
   * một tenant cụ thể, và mọi truy vấn vẫn đi qua đúng đường lọc tenant. Nhờ vậy ngày thêm tenant
   * thứ hai không có đường code nào chưa từng được chạy thử.
   */
  public static final UUID DEFAULT_TENANT = UUID.fromString("00000000-0000-0000-0000-000000000001");

  private static final ThreadLocal<UUID> CURRENT = new ThreadLocal<>();

  private TenantContext() {
  }

  /**
   * @throws IllegalStateException nếu chưa có tenant — đây là lỗi lập trình (quên đặt ở biên vào),
   *                               không phải tình huống nghiệp vụ, nên phải nổ chứ không được đoán
   */
  public static UUID require() {
    UUID tenantId = CURRENT.get();
    if (tenantId == null) {
      throw new IllegalStateException(
          "Chưa có tenant trong luồng hiện tại. Tenant phải được đặt ở biên vào "
              + "(filter HTTP hoặc Kafka listener). Trả về một giá trị mặc định ở đây sẽ biến "
              + "một lỗi lập trình thành một vụ rò rỉ dữ liệu chéo tenant.");
    }
    return tenantId;
  }

  public static Optional<UUID> current() {
    return Optional.ofNullable(CURRENT.get());
  }

  /** Chạy {@code action} trong ngữ cảnh của {@code tenantId}, luôn khôi phục giá trị cũ sau đó. */
  public static <T> T callWith(UUID tenantId, Supplier<T> action) {
    UUID previous = CURRENT.get();
    String previousMdc = MDC.get(MDC_KEY);
    set(tenantId);
    try {
      return action.get();
    } finally {
      restore(previous, previousMdc);
    }
  }

  public static void runWith(UUID tenantId, Runnable action) {
    callWith(tenantId, () -> {
      action.run();
      return null;
    });
  }

  /**
   * Đặt tenant thủ công. CHỈ dùng ở biên vào, và phải đi kèm {@link #clear()} trong khối
   * {@code finally} — nếu không, thread trong pool sẽ mang tenant này sang request kế tiếp.
   * Ưu tiên {@link #callWith} để không phải nhớ điều đó.
   */
  public static void set(UUID tenantId) {
    if (tenantId == null) {
      throw new IllegalArgumentException("tenantId không được null");
    }
    CURRENT.set(tenantId);
    MDC.put(MDC_KEY, tenantId.toString());
  }

  public static void clear() {
    CURRENT.remove();
    MDC.remove(MDC_KEY);
  }

  private static void restore(UUID previous, String previousMdc) {
    if (previous == null) {
      CURRENT.remove();
    } else {
      CURRENT.set(previous);
    }
    if (previousMdc == null) {
      MDC.remove(MDC_KEY);
    } else {
      MDC.put(MDC_KEY, previousMdc);
    }
  }
}
