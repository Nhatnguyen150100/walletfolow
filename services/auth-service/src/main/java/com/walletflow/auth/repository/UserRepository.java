package com.walletflow.auth.repository;

import com.walletflow.auth.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

/**
 * Tra theo email LUÔN kèm tenant: email chỉ unique trong một tenant (V2), nên
 * {@code findByEmail(email)} không còn nghĩa — nó có thể trả về người dùng của tenant khác. Cố ý
 * không giữ biến thể đó, cùng lý do với {@code AccountRepository} của ledger.
 */
public interface UserRepository extends JpaRepository<User, UUID> {
  Optional<User> findByTenantIdAndEmail(UUID tenantId, String email);

  boolean existsByTenantIdAndEmail(UUID tenantId, String email);
}
