package com.walletflow.common.outbox.readers;

import com.walletflow.common.outbox.OutboxEvent;
import com.walletflow.common.outbox.interfaces.OutboxReader;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.TypedQuery;
import org.hibernate.LockMode;
import org.hibernate.query.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class JpaOutboxReader implements OutboxReader {

  @PersistenceContext
  private EntityManager entityManager;

  /**
   * Đọc batch outbox chưa gửi và KHOÁ HÀNG bằng {@code SELECT ... FOR UPDATE SKIP LOCKED}.
   *
   * <p>Khi chạy nhiều instance relay song song: mỗi instance khoá một tập hàng rời nhau, hàng đang
   * bị instance khác khoá sẽ bị BỎ QUA (skip) thay vì chờ → không relay nào đọc trùng tập hàng nên
   * message không bị publish lặp. Scale ngang được mà vẫn an toàn.
   *
   * <p>Lock chỉ có hiệu lực đến hết transaction, nên phương thức này BẮT BUỘC được gọi bên trong
   * transaction của {@code OutboxRelay.publishPending()} (nơi cũng set {@code published=true}).
   */
  @Override
  public List<OutboxEvent> getBatch() {
    TypedQuery<OutboxEvent> query = entityManager.createQuery(
            "SELECT e FROM OutboxEvent e WHERE e.published = false ORDER BY e.createdAt ASC",
            OutboxEvent.class)
        .setMaxResults(100);
    query.unwrap(Query.class).setHibernateLockMode(LockMode.UPGRADE_SKIPLOCKED);
    return query.getResultList();
  }

  @Override
  public long countPending() {
    return entityManager.createQuery(
            "SELECT count(e) FROM OutboxEvent e WHERE e.published = false", Long.class)
        .getSingleResult();
  }

  @Override
  public Optional<Instant> oldestPendingCreatedAt() {
    return Optional.ofNullable(entityManager.createQuery(
            "SELECT min(e.createdAt) FROM OutboxEvent e WHERE e.published = false", Instant.class)
        .getSingleResult());
  }

  /**
   * Xoá theo lô bằng native SQL vì JPQL không có {@code LIMIT}.
   *
   * <p>Giới hạn số hàng là có chủ ý: bảng outbox của một hệ thống chạy lâu có thể có hàng triệu
   * dòng đã gửi, và một lệnh {@code DELETE} duy nhất trên ngần ấy hàng sẽ giữ khoá lâu, thổi phồng
   * WAL, và làm nghẽn chính đường ghi outbox của giao dịch đang chạy. Dọn dẹp không bao giờ được
   * phép cản trở đường đi của tiền.
   *
   * <p>Chỉ xoá hàng {@code published = true}: hàng chưa gửi là message CHƯA ĐẾN ĐÍCH, cũ đến mấy
   * cũng không được đụng tới.
   */
  @Override
  public int deletePublishedBefore(Instant cutoff, int batchSize) {
    return entityManager.createNativeQuery("""
            DELETE FROM outbox_event
            WHERE id IN (
                SELECT id FROM outbox_event
                WHERE published = true AND published_at < :cutoff
                ORDER BY published_at
                LIMIT :batchSize
            )
            """)
        .setParameter("cutoff", cutoff)
        .setParameter("batchSize", batchSize)
        .executeUpdate();
  }
}
