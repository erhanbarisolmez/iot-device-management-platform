package com.selftech.smartlock.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.selftech.smartlock.models.entity.Vehicle;

@Repository
public interface VehicleRepository extends JpaRepository <Vehicle, Long> {
  Optional<Vehicle> findByPlate_PlateNumber(String plateNumber);

}
