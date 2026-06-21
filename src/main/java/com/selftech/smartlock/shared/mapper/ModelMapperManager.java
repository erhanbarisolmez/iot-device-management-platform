package com.selftech.smartlock.shared.mapper;

import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;
import org.springframework.stereotype.Service;

@Service
public class ModelMapperManager implements ModelMapperService {

    private final ModelMapper modelMapper;

    public ModelMapperManager(ModelMapper modelMapper) {
        this.modelMapper = modelMapper;
    }

    @Override
    public ModelMapper forRequest() {
        modelMapper.getConfiguration()
            .setAmbiguityIgnored(true)
            .setMatchingStrategy(MatchingStrategies.STANDARD);
        return modelMapper;
    }

    @Override
    public ModelMapper forResponse() {
        modelMapper.getConfiguration()
            .setAmbiguityIgnored(true)
            .setMatchingStrategy(MatchingStrategies.LOOSE);
        return modelMapper;
    }

    @Override
    public ModelMapper forUpdate() {
        modelMapper.getConfiguration()
            .setAmbiguityIgnored(true)
            .setMatchingStrategy(MatchingStrategies.STANDARD)
            .setSkipNullEnabled(true);
        return modelMapper;
    }
}
