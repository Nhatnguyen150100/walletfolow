package com.walletflow.ledger.repositories;

import com.walletflow.ledger.entity.Account;
import com.walletflow.ledger.enums.ECurrency;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface AccountRepository extends JpaRepository<Account, UUID> {
    Optional<Account> findByExternalRefAndCurrency(String externalRef, ECurrency currency);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT a FROM Account a WHERE a.externalRef = :ref AND a.currency = :currency")
    Optional<Account> findForUpdate(@Param("ref") String ref, @Param("currency") ECurrency currency);
}
