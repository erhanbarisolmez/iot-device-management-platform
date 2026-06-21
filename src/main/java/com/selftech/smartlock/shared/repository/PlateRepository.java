package com.selftech.smartlock.shared.repository;

import com.selftech.smartlock.shared.model.Plate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PlateRepository extends JpaRepository<Plate, Long> {
    Plate findPlateByPlateNumber(String plateNumber);
}
