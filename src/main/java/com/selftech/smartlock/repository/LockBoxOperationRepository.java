package com.selftech.smartlock.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.selftech.smartlock.models.entity.BoxOperation;

@Repository
public interface LockBoxOperationRepository extends JpaRepository<BoxOperation, Long> {
    
}