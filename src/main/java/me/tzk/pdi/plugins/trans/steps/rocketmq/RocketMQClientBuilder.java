package me.tzk.pdi.plugins.trans.steps.rocketmq;

import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.google.common.collect.ImmutableMap;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.trans.step.StepInterface;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class RocketMQClientBuilder {
    private static final Class<?> PKG = RocketMQClientBuilder.class;

    protected Consumer client;

    private static final String UNSECURE_PROTOCOL = "http://";
    private static final String SECURE_PROTOCOL = "ssl://";

    public static final Map<String, String> DEFAULT_SSL_OPTS = ImmutableMap.<String, String>builder()
            .put( "ssl.protocol", "TLS" )
            .put( "ssl.contextProvider", "" )
            .put( "ssl.keyStore", "" )
            .put( "ssl.keyStorePassword", "" )
            .put( "ssl.keyStoreType", "JKS" )
            .put( "ssl.keyStoreProvider", "" )
            .put( "ssl.trustStore", "" )
            .put( "ssl.trustStorePassword", "" )
            .put( "ssl.trustStoreType", "" )
            .put( "ssl.trustStoreProvider", "" )
            .put( "ssl.enabledCipherSuites", "" )
            .put( "ssl.keyManager", "" )
            .put( "ssl.trustManager", "" )
            .build();

    private String onsAddr;
    private String customerId;
    private String topic;
    private List<String> tags;
    private String accessKey;
    private String secretKey;
    private boolean isSecure = false;
    private Map<String, String> sslConfig;
    private String messageModel;
    private String consumeThreadNums;
    private String maxReconsumeTimes;
    private String consumeTimeout;
    private String consumeMessageBatchMaxSize;
    private String checkImmunityTimeInSeconds;
    private String shardingKey;
    private String suspendTimeMillis;
    private MessageListener callback;
    private LogChannelInterface logChannel;
    private String stepName;

    private RocketMQClientBuilder() {
    }

    public static RocketMQClientBuilder build() {
        return new RocketMQClientBuilder();
    }

    RocketMQClientBuilder withCallback(MessageListener callback) {
        this.callback = callback;
        return this;
    }

    public RocketMQClientBuilder withONSAddr (String onsAddr) {
        this.onsAddr = onsAddr;
        return this;
    }

    public RocketMQClientBuilder withCustomerId (String customerId) {
        this.customerId = customerId;
        return this;
    }

    RocketMQClientBuilder withTopic( String topic ) {
        this.topic = topic;
        return this;
    }

    RocketMQClientBuilder withTags( List<String> tags ) {
        this.tags = tags;
        return this;
    }

    public RocketMQClientBuilder withAccessKey( String accessKey ) {
        this.accessKey = accessKey;
        return this;
    }

    public RocketMQClientBuilder withSecretKey( String secretKey ) {
        this.secretKey = secretKey;
        return this;
    }

    public RocketMQClientBuilder withStep( StepInterface step ) {
        this.logChannel = step.getLogChannel();
        this.stepName = step.getStepMeta().getName();
        return this;
    }

    public RocketMQClientBuilder withIsSecure( boolean isSecure ) {
        this.isSecure = isSecure;
        return this;
    }

    RocketMQClientBuilder withSslConfig( Map<String, String> sslConfig ) {
        this.sslConfig = sslConfig;
        return this;
    }

    public RocketMQClientBuilder withMessageModel(String messageModel) {
        this.messageModel = messageModel;
        return this;
    }

    public RocketMQClientBuilder withConsumeThreadNums(String consumeThreadNums) {
        this.consumeThreadNums = consumeThreadNums;
        return this;
    }

    public RocketMQClientBuilder withMaxReconsumeTimes(String maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
        return this;
    }

    public RocketMQClientBuilder withConsumeTimeout(String consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
        return this;
    }

    public RocketMQClientBuilder withConsumeMessageBatchMaxSize(String consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
        return this;
    }

    public RocketMQClientBuilder withCheckImmunityTimeInSeconds(String checkImmunityTimeInSeconds) {
        this.checkImmunityTimeInSeconds = checkImmunityTimeInSeconds;
        return this;
    }

    public RocketMQClientBuilder withShardingKey(String shardingKey) {
        this.shardingKey = shardingKey;
        return this;
    }

    public RocketMQClientBuilder withSuspendTimeMillis(String suspendTimeMillis) {
        this.suspendTimeMillis = suspendTimeMillis;
        return this;
    }

    public Consumer createConsumer() {
        validateArgs();

        String targetAddr = getProtocol() + onsAddr;

        if ( StringUtil.isEmpty( customerId ) ) {
            logChannel.logDebug( "Customer ID cannot be null." );
        }

        Properties clientProperties = new Properties();
        clientProperties.setProperty(PropertyKeyConst.ConsumerId, customerId);
        clientProperties.setProperty(PropertyKeyConst.AccessKey, accessKey);
        clientProperties.setProperty(PropertyKeyConst.SecretKey, secretKey);
        clientProperties.setProperty(PropertyKeyConst.ONSAddr, targetAddr);
        client = ONSFactory.createConsumer(clientProperties);
        client.subscribe(topic, buildTag(tags), callback);

        logChannel.logBasic( "RocketMQ stream source starts success.");
        logChannel.logDebug( "Subscribing to topic: " + topic + " with tags: " + buildTag(tags) );
        return client;
    }

    private String getProtocol() {
        return isSecure ? SECURE_PROTOCOL : UNSECURE_PROTOCOL;
    }

    private void validateArgs() {
        // check any conditions
    }

    private String buildTag(List<String> tags) {
        for( String aTag: tags) {
            if( "*".equals(aTag)) {
                return "*";
            }
        }
        return String.join("||", tags);
    }
}
