package com.selftech.smartlock.service.concretes;

import java.time.LocalDateTime;

import org.springframework.stereotype.Service;

import com.selftech.smartlock.shared.model.Plate;
import com.selftech.smartlock.shared.repository.PlateRepository;
import com.selftech.smartlock.models.entity.Vehicle;
import com.selftech.smartlock.repository.VehicleRepository;
import com.selftech.smartlock.service.abstracts.IVehicleService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class VehicleManager implements IVehicleService {

  private final VehicleRepository vehicleRepository;
  private final PlateRepository plateRepository;

  public Vehicle createVehicleForPlate(String plateNumber) {
    Plate plate = plateRepository.findPlateByPlateNumber(plateNumber);
    log.info("Vehicle find={}", plate);
    if (plate == null) {
      log.error("Vehicle find null={}", plate);
      throw new IllegalArgumentException("Geçerli plaka bulunamadı " + plateNumber);
    }

    Vehicle vehicle = new Vehicle();
    vehicle.setPlate(plate);
    vehicle.setUser(plate.getUser());
    vehicle.setCreatedAt(LocalDateTime.now());
    log.info("vehicle create =  {}", vehicle);
    return vehicleRepository.save(vehicle);
  }
}
