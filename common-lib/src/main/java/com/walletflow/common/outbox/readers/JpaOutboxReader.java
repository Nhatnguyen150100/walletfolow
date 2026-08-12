package com.walletflow.common.outbox.readers;

import com.walletflow.common.outbox.OutboxEvent;
import com.walletflow.common.outbox.interfaces.OutboxReader;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.TypedQuery;
import org.hibernate.LockMode;
import org.hibernate.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

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
}
