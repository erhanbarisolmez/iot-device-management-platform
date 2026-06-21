package com.selftech.smartlock.service.abstracts;

import java.util.List;

import com.selftech.selfparkbackendv001.models.user.User;
import com.selftech.smartlock.models.entity.LockBox;

public interface ILockBoxService {
    LockBox createReturnBox(LockBox lockBox, User user);

    LockBox updateReturnBox(LockBox lockBox, User user);

    List<LockBox> getAllReturnBoxes();

    void deleteReturnBox(Long id, User user);

    String generateOpeningCode(String boxCode, User user);

    boolean openBoxWithCode(String boxCode, String code, User user);

    boolean validateOpeningCode(String boxCode, String code); // Bu metodun implementasyonu yok, kaldırılabilir.
}