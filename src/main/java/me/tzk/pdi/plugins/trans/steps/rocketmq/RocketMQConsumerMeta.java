package me.tzk.pdi.plugins.trans.steps.rocketmq;

import com.google.common.collect.ImmutableMap;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.util.serialization.Sensitive;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.metastore.api.IMetaStore;

import java.util.*;

import static java.util.stream.Collectors.toList;
import static me.tzk.pdi.plugins.trans.steps.rocketmq.RocketMQConstants.ONS_ADDR;
import static org.pentaho.di.core.util.serialization.ConfigHelper.conf;
import static org.pentaho.di.i18n.BaseMessages.getString;


@Step( id = "RocketMQConsumer", image = "RocketMQConsumer.svg",
        i18nPackageName = "me.tzk.pdi.plugins.trans.step.rocketmq",
        name = "RocketMQConsumer.TypeLongDesc",
        description = "RocketMQConsumer.TypeTooltipDesc",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Streaming",
        documentationUrl = "Products/Data_Integration/Transformation_Step_Reference")
@InjectionSupported( localizationPrefix = "RocketMQConsumerMeta.Injection.", groups = { "CONNECTION_PROPS","SSL", "CONSUME_OPTS" } )
public class RocketMQConsumerMeta extends BaseStreamStepMeta implements StepMetaInterface {
    private static Class<?> PKG = RocketMQConsumerMeta.class;
    
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

    @Injection( name = "ONS_ADDR", group = "CONNECTION_PROPS" ) private String ONSAddr = "";
    @Injection( name = "CONSUMER_ID", group = "CONNECTION_PROPS" ) private String consumerId = "";
    @Injection( name = "TOPIC", group = "CONNECTION_PROPS" ) private String topic = "";
    @Injection( name = "TAGS", group = "CONNECTION_PROPS" ) private List<String> tags = new ArrayList<String>();
    @Sensitive
    @Injection( name = "ACCESS_KEY", group = "CONNECTION_PROPS" ) private String accessKey = "";
    @Sensitive
    @Injection( name = "SECRET_KEY", group = "CONNECTION_PROPS" ) private String secretKey = "";

    @Injection ( name = "USE_SSL", group = "SSL" ) private Boolean useSsl = false;
    @Injection ( name = "SSL_KEYS", group = "SSL" ) public List<String> sslKeys = new ArrayList<>();
    @Sensitive
    @Injection ( name = "SSL_VALUES", group = "SSL" ) public List<String> sslValues = new ArrayList<>();

    @Injection ( name = "TOPIC_OUTPUT_NAME" ) private String topicOutputName = "Topic";
    @Injection ( name = "TAG_OUTPUT_NAME" ) private String tagOutputName = "Tag";
    @Injection ( name = "MSG_ID_OUTPUT_NAME" ) private String msgIdOutputName = "MessageID";
    @Injection ( name = "BODY_OUTPUT_NAME" ) private String bodyOutputName = "Content";

    @Injection ( name = "MESSAGE_MODEL", group = "CONSUME_OPTS" ) private String messageModel = "";
    @Injection ( name = "CONSUME_THREAD_NUMS", group = "CONSUME_OPTS" ) private String consumeThreadNums = "";
    @Injection ( name = "MAX_RECONSUME_TIMES", group = "CONSUME_OPTS" ) private String maxReconsumeTimes = "";
    @Injection ( name = "CONSUME_TIMEOUT", group = "CONSUME_OPTS" ) private String consumeTimeout = "";
    @Injection ( name = "CONSUME_MESSAGE_BATCH_MAX_SIZE", group = "CONSUME_OPTS" ) private String consumeMessageBatchMaxSize = "";
    @Injection ( name = "CHECK_IMMUNITY_TIME_IN_SECONDS", group = "CONSUME_OPTS"  ) private String checkImmunityTimeInSeconds = "";
    @Injection ( name = "SHARDING_KEY", group = "CONSUME_OPTS"  ) private String shardingKey = "";
    @Injection ( name = "SUSPEND_TIME_MILLIS", group = "CONSUME_OPTS" ) private String suspendTimeMillis = "";


    //
    //Constructor
    //
    public RocketMQConsumerMeta() {
        super();
        setSpecificationMethod( ObjectLocationSpecificationMethod.FILENAME );
    }

    public void setDefault() {
        super.setDefault();
        //set connection parameters
        ONSAddr = "";
        consumerId = "";
        topic = "";
        tags.add("*");
        accessKey = "";
        secretKey = "";
        //set SSL parameters
        sslKeys = DEFAULT_SSL_OPTS
                .keySet().stream()
                .sorted()
                .collect( toList() );
        sslValues = sslKeys.stream()
                .map( DEFAULT_SSL_OPTS::get )
                .collect( toList() );
        //set option parameters
        messageModel = "CLUSTERING";
        consumeThreadNums = "";
        maxReconsumeTimes = "";
        consumeTimeout = "";
        consumeMessageBatchMaxSize = "";
        checkImmunityTimeInSeconds = "";
        shardingKey = "";
        suspendTimeMillis = "";
    }

    @Override
    public String getFileName() {
        return getTransformationPath();
    }

    @Override
    public RowMeta getRowMeta(String origin, VariableSpace space ) {
        RowMeta rowMeta = new RowMeta();
        rowMeta.addValueMeta( new ValueMetaString( msgIdOutputName ) );
        rowMeta.addValueMeta( new ValueMetaString( topicOutputName ) );
        rowMeta.addValueMeta( new ValueMetaString( tagOutputName ) );
        rowMeta.addValueMeta( new ValueMetaString( bodyOutputName ) );
        return rowMeta;
    }

    public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info,
                          StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore)
            throws KettleStepException {

        /*
         * This implementation appends the outputField to the row-stream
         */
        // a value meta object contains the meta data for a field
        ValueMetaInterface f1 = new ValueMetaString(msgIdOutputName);
        ValueMetaInterface f2 = new ValueMetaString(topicOutputName);
        ValueMetaInterface f3 = new ValueMetaString(tagOutputName);
        ValueMetaInterface f4 = new ValueMetaString(bodyOutputName);

        // setting trim type to "both"
        f1.setTrimType(ValueMeta.TRIM_TYPE_BOTH);
        f2.setTrimType(ValueMeta.TRIM_TYPE_BOTH);
        f3.setTrimType(ValueMeta.TRIM_TYPE_BOTH);
        f4.setTrimType(ValueMeta.TRIM_TYPE_BOTH);

        // the name of the step that adds this field
        f1.setOrigin(name);
        f2.setOrigin(name);
        f3.setOrigin(name);
        f4.setOrigin(name);

        // modify the row structure and add the field this step generates
        inputRowMeta.addValueMeta(f1);
        inputRowMeta.addValueMeta(f2);
        inputRowMeta.addValueMeta(f3);
        inputRowMeta.addValueMeta(f4);
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans ) {
        return new RocketMQConsumer(stepMeta, stepDataInterface, cnr, tr, trans);
    }

    public StepDataInterface getStepData() {
        return new RocketMQConsumerData();
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name ) {
        return new RocketMQConsumerDialog( shell, meta, transMeta, name );
    }

    public List<StepOption> retrieveOptions() {
        return Arrays.asList(
                new StepOption( RocketMQConstants.MESSAGE_MODEL, getString( PKG, "RocketMQDialog.Options.MESSAGE_MODEL" ),
                        messageModel),
                new StepOption( RocketMQConstants.CONSUME_THREAD_NUMS, getString( PKG, "RocketMQDialog.Options.CONSUME_THREAD_NUMS" ),
                        consumeThreadNums),
                new StepOption( RocketMQConstants.MAX_RECONSUME_TIMES, getString( PKG, "RocketMQDialog.Options.MAX_RECONSUME_TIMES" ),
                        maxReconsumeTimes),
                new StepOption( RocketMQConstants.CONSUME_TIMEOUT, getString( PKG, "RocketMQDialog.Options.CONSUME_TIMEOUT" ),
                        consumeTimeout),
                new StepOption( RocketMQConstants.CONSUME_MESSAGE_BATCH_MAX_SIZE, getString( PKG, "RocketMQDialog.Options.CONSUME_MESSAGE_BATCH_MAX_SIZE" ),
                        consumeMessageBatchMaxSize),
                new StepOption( RocketMQConstants.CHECK_IMMUNITY_TIME_IN_SECONDS, getString( PKG, "RocketMQDialog.Options.CHECK_IMMUNITY_TIME_IN_SECONDS" ),
                        checkImmunityTimeInSeconds),
                new StepOption( RocketMQConstants.SHARDING_KEY, getString( PKG, "RocketMQDialog.Options.SHARDING_KEY" ),
                        shardingKey),
                new StepOption( RocketMQConstants.SUSPEND_TIME_MILLIS, getString( PKG, "RocketMQDialog.Options.SUSPEND_TIME_MILLIS" ),
                        suspendTimeMillis)
        );
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
                      StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output,
                      RowMetaInterface info, VariableSpace space, Repository repository,
                      IMetaStore metaStore ) {
        super.check( remarks, transMeta, stepMeta, prev, input, output, info, space, repository, metaStore );

        StepOption.checkInteger( remarks, stepMeta, space, getString( PKG, "RocketMQDialog.Options.CONSUME_THREAD_NUMS" ),
                consumeThreadNums );
        StepOption.checkInteger( remarks, stepMeta, space, getString( PKG, "RocketMQDialog.Options.MAX_RECONSUME_TIMES" ),
                maxReconsumeTimes );
        StepOption.checkInteger( remarks, stepMeta, space, getString( PKG, "RocketMQDialog.Options.CONSUME_TIMEOUT" ),
                consumeTimeout );
        StepOption.checkInteger( remarks, stepMeta, space, getString( PKG, "RocketMQDialog.Options.CONSUME_MESSAGE_BATCH_MAX_SIZE" ),
                consumeMessageBatchMaxSize );
        StepOption.checkInteger( remarks, stepMeta, space, getString( PKG, "RocketMQDialog.Options.CHECK_IMMUNITY_TIME_IN_SECONDS" ),
                checkImmunityTimeInSeconds );
        StepOption.checkInteger( remarks, stepMeta, space, getString( PKG, "RocketMQDialog.Options.SHARDING_KEY" ),
                shardingKey );
        StepOption.checkInteger( remarks, stepMeta, space, getString( PKG, "RocketMQDialog.Options.SUSPEND_TIME_MILLIS" ),
                suspendTimeMillis );
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        RocketMQConsumerMeta that = (RocketMQConsumerMeta) o;
        return Objects.equals( useSsl, that.useSsl )
                && Objects.equals( sslKeys, that.sslKeys )
                && Objects.equals( sslValues, that.sslValues )
                && Objects.equals( ONSAddr, that.ONSAddr )
                && Objects.equals( topic, that.topic )
                && Objects.equals( tags, that.tags )
                && Objects.equals( consumerId, that.consumerId )
                && Objects.equals( accessKey, that.accessKey )
                && Objects.equals( secretKey, that.secretKey )
                && Objects.equals( msgIdOutputName, that.msgIdOutputName )
                && Objects.equals( topicOutputName, that.topicOutputName )
                && Objects.equals( tagOutputName, that.tagOutputName )
                && Objects.equals( bodyOutputName, that.bodyOutputName )
                && Objects.equals( messageModel, that.messageModel )
                && Objects.equals( consumeThreadNums, that.consumeThreadNums )
                && Objects.equals( maxReconsumeTimes, that.maxReconsumeTimes )
                && Objects.equals( consumeTimeout, that.consumeTimeout )
                && Objects.equals( consumeMessageBatchMaxSize, that.consumeMessageBatchMaxSize )
                && Objects.equals( checkImmunityTimeInSeconds, that.checkImmunityTimeInSeconds )
                && Objects.equals( shardingKey, that.shardingKey )
                && Objects.equals( suspendTimeMillis, that.suspendTimeMillis );
    }

    @Override public int hashCode() {
        return Objects
                .hash( ONSAddr, topic, tags, consumerId, accessKey, secretKey,
                        msgIdOutputName, topicOutputName, tagOutputName, bodyOutputName,
                        useSsl, sslKeys, sslValues,
                        messageModel, consumeThreadNums,maxReconsumeTimes, consumeTimeout,
                        consumeMessageBatchMaxSize, checkImmunityTimeInSeconds, shardingKey, suspendTimeMillis );
    }



    //////////////////////////////////////////////////////////////
    // getter and setter
    //////////////////////////////////////////////////////////////
    public String getONSAddr() {
        return ONSAddr;
    }

    public void setONSAddr(String ONSAddr) {
        this.ONSAddr = ONSAddr;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public Boolean isUseSsl() {
        return useSsl;
    }

    public void setUseSsl(Boolean useSsl) {
        this.useSsl = useSsl;
    }

    public Map<String, String> getSslConfig() {
        return conf( sslKeys, sslValues ).asMap();
    }

    public void setSslConfig( Map<String, String> sslConfig ) {
        sslKeys = conf( sslConfig ).keys();
        sslValues = conf( sslConfig ).vals();
    }

    public String getSuspendTimeMillis() {
        return suspendTimeMillis;
    }

    public void setSuspendTimeMillis(String suspendTimeMillis) {
        this.suspendTimeMillis = suspendTimeMillis;
    }

    public String getConsumeTimeout() {
        return consumeTimeout;
    }

    public void setConsumeTimeout(String consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }

    public String getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(String maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public String getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(String messageModel) {
        this.messageModel = messageModel;
    }

    public String getConsumeThreadNums() {
        return consumeThreadNums;
    }

    public void setConsumeThreadNums(String consumeThreadNums) {
        this.consumeThreadNums = consumeThreadNums;
    }

    public String getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(String consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }

    public String getCheckImmunityTimeInSeconds() {
        return checkImmunityTimeInSeconds;
    }

    public void setCheckImmunityTimeInSeconds(String checkImmunityTimeInSeconds) {
        this.checkImmunityTimeInSeconds = checkImmunityTimeInSeconds;
    }

    public String getShardingKey() {
        return shardingKey;
    }

    public void setShardingKey(String shardingKey) {
        this.shardingKey = shardingKey;
    }

    public String getMsgIdOutputName() {
        return msgIdOutputName;
    }

    public void setMsgIdOutputName(String msgIdOutputName) {
        this.msgIdOutputName = msgIdOutputName;
    }

    public String getBodyOutputName() {
        return bodyOutputName;
    }

    public void setBodyOutputName(String bodyOutputName) {
        this.bodyOutputName = bodyOutputName;
    }

    public String getTagOutputName() {
        return tagOutputName;
    }

    public void setTagOutputName(String tagOutputName) {
        this.tagOutputName = tagOutputName;
    }

    public String getTopicOutputName() {
        return topicOutputName;
    }

    public void setTopicOutputName(String topicOutputName) {
        this.topicOutputName = topicOutputName;
    }


}
