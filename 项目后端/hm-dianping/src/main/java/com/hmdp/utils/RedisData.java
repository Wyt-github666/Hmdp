package com.hmdp.utils;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Builder
public class RedisData implements Serializable {
    private LocalDateTime expireTime;
    private Object data;
}
