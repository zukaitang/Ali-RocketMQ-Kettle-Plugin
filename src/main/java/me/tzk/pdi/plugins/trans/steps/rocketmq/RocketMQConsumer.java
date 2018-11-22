package me.tzk.pdi.plugins.trans.steps.rocketmq;

import com.google.common.base.Preconditions;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStep;
import org.pentaho.di.trans.streaming.common.FixedTimeStreamWindow;

import static org.pentaho.di.i18n.BaseMessages.getString;

public class RocketMQConsumer extends BaseStreamStep implements StepInterface {
    private static Class<?> PKG = RocketMQConsumer.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    public RocketMQConsumer(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
        super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    }

    public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
        boolean init = super.init( stepMetaInterface, stepDataInterface );
        Preconditions.checkNotNull( stepMetaInterface );
        RocketMQConsumerMeta rocketMQConsumerMeta = (RocketMQConsumerMeta) stepMetaInterface;

        try {
            RowMeta rowMeta = rocketMQConsumerMeta.getRowMeta( getStepname(), this );
            window = new FixedTimeStreamWindow<>( subtransExecutor, rowMeta, getDuration(), getBatchSize() );
            source = new RocketMQStreamSource( rocketMQConsumerMeta, this );
        } catch ( Exception e ) {
            getLogChannel().logError( getString( PKG, "RocketMQInput.Error.FailureGettingFields" ), e );
            init = false;
        }
        return init;
    }
}
