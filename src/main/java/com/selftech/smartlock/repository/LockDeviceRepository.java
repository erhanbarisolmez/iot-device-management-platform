package com.selftech.smartlock.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import com.selftech.smartlock.models.entity.LockDevice;

public interface LockDeviceRepository extends JpaRepository<LockDevice, Long> {

  Optional<LockDevice> findByDeviceCode(String deviceCode);

  List<LockDevice> findByCurrentBox_BoxCode(String boxCode);

  boolean existsByDeviceCode(String deviceCode);


} 