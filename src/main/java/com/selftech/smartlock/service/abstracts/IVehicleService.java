 package com.selftech.smartlock.service.abstracts;

import com.selftech.smartlock.models.entity.Vehicle;

public interface IVehicleService {
   Vehicle createVehicleForPlate(String plateNumber);
}