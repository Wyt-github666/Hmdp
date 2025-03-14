package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisWork;
import com.hmdp.utils.SimpleRedisLoock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private RedisWork redisWork;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    private IVoucherOrderService iVoucherOrderService;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<VoucherOrder>(1024 * 1024);

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private static final String queueName = "stream.orders";

    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            log.info("开始处理异步任务：秒杀卷订单");
            while (true) {
                try {
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1","c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    if(read == null || read.isEmpty()){
                        continue;
                    }
                    MapRecord<String,Object,Object> record = read.get(0);
                    Map<Object, Object> value = record.getValue();
                    System.out.println(value);
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    System.out.println(voucherOrder);
//                    VoucherOrder take = orderTasks.take();
                    handleVoucherOrder(voucherOrder);
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("订单异常：{}", e.getMessage());
                    handlePendingList();
                }
            }
        }
    }

    private void handlePendingList() {
        while (true) {
            try{
                List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(queueName, ReadOffset.from("0"))
                );
                if(read == null || read.isEmpty()){
                    break;
                }
                MapRecord<String,Object,Object> record = read.get(0);
                Map<Object, Object> value = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                handleVoucherOrder(voucherOrder);
                stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
            }catch (Exception e){
                log.error("异常信息{}", e.getMessage());
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("重复下单:优惠卷id{}，用户id{}",voucherOrder.getVoucherId(), userId);
            return;
        }try{
            iVoucherOrderService.createVoucherOrder(voucherOrder);
        } catch (Exception e) {
            log.error("订单异常信息：{}", e.getMessage());
            throw new RuntimeException(e);
        }finally {
            lock.unlock();
        }
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
//        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始");
//        }
//        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束");
//        }
//        if (seckillVoucher.getStock() < 1){
//            return Result.fail("库存不足");
//        }
//        Long userId = UserHolder.getUser().getId();
//        String name = "lock:order:" + userId;
////        synchronized (userId.toString().intern()) {
////        SimpleRedisLoock simpleRedisLoock = new SimpleRedisLoock(name,stringRedisTemplate);
//        RLock lock = redissonClient.getLock(name);
//        boolean isLock = lock.tryLock();
////        boolean isLock = simpleRedisLoock.tryLock(5);
//        if(!isLock){
//            return Result.fail("操作频繁");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } catch (IllegalStateException e) {
//            throw new RuntimeException(e);
//        }finally {
////            simpleRedisLoock.unlock();
//            lock.unlock();
//        }
////        }
//    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        // 1 执行lua脚本
//        int result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        ).intValue();
//        if (result != 0) {
//            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
//        }
//        iVoucherOrderService = (IVoucherOrderService) AopContext.currentProxy();
//        // 2 判断是否为0
//
//        // 为0 表示有购买资格，添加到阻塞队列当中
//        // 返回订单id
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderId = redisWork.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setVoucherId(voucherId);
//        voucherOrder.setUserId(userId);
//        orderTasks.add(voucherOrder);
//        return Result.ok(orderId);
//    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        long orderId = redisWork.nextId("order");
        Long userId = UserHolder.getUser().getId();
        // 1 执行lua脚本
        int result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        ).intValue();
        if (result != 0) {
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }
        iVoucherOrderService = (IVoucherOrderService) AopContext.currentProxy();
        // 2 判断是否为0

        // 为0 表示有购买资格，添加到阻塞队列当中
        // 返回订单id
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(userId);
//        orderTasks.add(voucherOrder);
        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Integer count = query().eq("user_id",userId).eq("voucher_id",voucherOrder.getVoucherId()).count();
        if(count > 0){
            return;
        }
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock",0).update();
        if (!success){
            return;
        }
        save(voucherOrder);
    }
}
