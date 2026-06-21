package com.selftech.smartlock.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.selftech.smartlock.models.entity.LockBox;

@Repository
public interface LockBoxRepository extends JpaRepository<LockBox, Long> {
  Optional<LockBox> findByBoxCode(String boxCode);

  Optional<LockBox> findByDevices_DeviceCode(String deviceCode);

  boolean existsByBoxCode(String boxCode);

  List<LockBox> findByLocation(String location);

  @Query("select b from LockBox b where (b.status = 'EMPTY' or b.status is null) and b.devices is empty")
  Optional<LockBox> findAvailableBox(); // TODO: findFirstAvailableBox olarak değiştirilebilir.
}
