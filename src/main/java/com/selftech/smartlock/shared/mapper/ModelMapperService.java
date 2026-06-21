package com.selftech.smartlock.shared.mapper;

import org.modelmapper.ModelMapper;

public interface ModelMapperService {
    ModelMapper forRequest();
    ModelMapper forResponse();
    ModelMapper forUpdate();
}
