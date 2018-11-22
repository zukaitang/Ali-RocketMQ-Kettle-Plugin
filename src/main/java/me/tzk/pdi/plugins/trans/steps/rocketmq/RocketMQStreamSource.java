package me.tzk.pdi.plugins.trans.steps.rocketmq;

import com.aliyun.openservices.ons.api.*;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.google.common.collect.ImmutableList;
import org.pentaho.di.trans.streaming.common.BlockingQueueStreamSource;

import java.util.Collections;
import java.util.List;

import static java.nio.charset.Charset.defaultCharset;

public class RocketMQStreamSource extends BlockingQueueStreamSource<List<Object>> {
    private static final Class<?> PKG = RocketMQStreamSource.class;
    private final RocketMQConsumerMeta rocketMQConsumerMeta;
    private final RocketMQConsumer rocketMQConsumer;

    protected Consumer client;

    private MessageListener callback = new MessageListener() {
        @Override
        public Action consume(Message message, ConsumeContext consumeContext) {
            acceptRows(Collections.singletonList(ImmutableList.of(
                    message.getMsgID(),
                    message.getTopic(),
                    message.getTag(),
                    new String(message.getBody(), defaultCharset())
            )));
            return Action.CommitMessage;
        }
    };

    RocketMQStreamSource(RocketMQConsumerMeta rocketMQConsumerMeta, RocketMQConsumer rocketMQConsumer) {
        super(rocketMQConsumer);
        this.rocketMQConsumerMeta = rocketMQConsumerMeta;
        this.rocketMQConsumer = rocketMQConsumer;
    }

    @Override
    public void open() {
        try {
            client = RocketMQClientBuilder.build()
                    .withStep(rocketMQConsumer)
                    .withONSAddr(rocketMQConsumerMeta.getONSAddr())
                    .withCustomerId(rocketMQConsumerMeta.getConsumerId())
                    .withTopic(rocketMQConsumerMeta.getTopic())
                    .withTags(rocketMQConsumerMeta.getTags())
                    .withAccessKey(rocketMQConsumerMeta.getAccessKey())
                    .withSecretKey(rocketMQConsumerMeta.getSecretKey())
                    .withCallback(callback)
                    .createConsumer();
            client.start();
            //等待固定时间防止进程退出
//            try {
//                Thread.sleep(200000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        } catch ( Exception e) {
            rocketMQConsumer.stopAll();
            rocketMQConsumer.logError(e.toString());
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            if (client != null && client.isStarted()) {
                client.shutdown();
            }
        } catch (ONSClientException exception) {
            rocketMQConsumer.logError(exception.getMessage());
        }
    }

}
