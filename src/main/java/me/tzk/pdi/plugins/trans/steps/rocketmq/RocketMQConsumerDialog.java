package me.tzk.pdi.plugins.trans.steps.rocketmq;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStreamingDialog;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

public class RocketMQConsumerDialog extends BaseStreamingDialog implements StepDialogInterface {
    private static Class<?> PKG = RocketMQConsumerMeta.class;  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    private RocketMQConsumerMeta rocketMQMeta;
    private TextVar wConnection;
    private TextVar wTopic;
    private TableView wTagsTable;
    private TextVar wConsumerId;
    private TableView fieldsTable;

    private final Point startingDimensions = new Point( 527, 676 );

    private RocketMQDialogSecurityLayout securityLayout;
    private RocketMQDialogOptionsLayout optionsLayout;

    public RocketMQConsumerDialog( Shell parent, Object in, TransMeta tr, String sname ) {
        super( parent, in, tr, sname );
        rocketMQMeta = (RocketMQConsumerMeta) in;
    }

    @Override
    protected void getData() {
        super.getData();
        wConnection.setText( rocketMQMeta.getONSAddr() );
        wTopic.setText( rocketMQMeta.getTopic());
        populateTagsData();
        wConsumerId.setText( rocketMQMeta.getConsumerId() );
        securityLayout.setUIText();
    }

    private void populateTagsData() {
        List<String> tags = rocketMQMeta.getTags();
        int rowIndex = 0;
        for ( String tag : tags ) {
            TableItem key = wTagsTable.getTable().getItem( rowIndex++ );
            if ( tag != null ) {
                key.setText( 1, tag );
            }
        }
    }

    @Override protected String getDialogTitle() {
        return BaseMessages.getString( PKG, "RocketMQConsumerDialog.Shell.Title");
    }

    @Override
    public void setSize() {
        setSize( shell );  // sets shell location and preferred size
        shell.setMinimumSize( startingDimensions );
        shell.setSize( startingDimensions ); // force initial size
    }

    @Override protected void buildSetup( Composite wSetupComp ) {
        props.setLook( wSetupComp );
        FormLayout setupLayout = new FormLayout();
        setupLayout.marginHeight = 15;
        setupLayout.marginWidth = 15;
        wSetupComp.setLayout( setupLayout );

        Label wlConnection = new Label( wSetupComp, SWT.LEFT );
        props.setLook( wlConnection );
        wlConnection.setText( BaseMessages.getString( PKG, "RocketMQConsumerDialog.Connection") );
        FormData fdlConnection = new FormData();
        fdlConnection.left = new FormAttachment( 0, 0 );
        fdlConnection.right = new FormAttachment( 50, 0 );
        fdlConnection.top = new FormAttachment( 0, 0 );
        wlConnection.setLayoutData( fdlConnection );

        wConnection = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wConnection );
        wConnection.addModifyListener( lsMod );
        FormData fdConnection = new FormData();
        fdConnection.left = new FormAttachment( 0, 0 );
        fdConnection.right = new FormAttachment( 0, 363 );
        fdConnection.top = new FormAttachment( wlConnection, 5 );
        wConnection.setLayoutData( fdConnection );

        Label wlConsumerId = new Label( wSetupComp, SWT.LEFT );
        props.setLook( wlConsumerId );
        wlConsumerId.setText( BaseMessages.getString( PKG, "RocketMQConsumerDialog.ConsumerId") );
        FormData fdlConsumerId = new FormData();
        fdlConsumerId.left = new FormAttachment( 0, 0 );
        fdlConsumerId.right = new FormAttachment( 50, 0 );
        fdlConsumerId.top = new FormAttachment( wConnection, 10 );
        wlConsumerId.setLayoutData( fdlConsumerId );

        wConsumerId = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wConsumerId );
        wConsumerId.addModifyListener( lsMod );
        FormData fdConsumerId = new FormData();
        fdConsumerId.left = new FormAttachment( 0, 0 );
        fdConsumerId.right = new FormAttachment( 0, 363 );
        fdConsumerId.top = new FormAttachment( wlConsumerId, 5 );
        wConsumerId.setLayoutData( fdConsumerId );

        Label wlTopic = new Label( wSetupComp, SWT.LEFT );
        props.setLook( wlTopic );
        wlTopic.setText( BaseMessages.getString( PKG, "RocketMQConsumerDialog.Topic") );
        FormData fdlTopics = new FormData();
        fdlTopics.left = new FormAttachment( 0, 0 );
        fdlTopics.right = new FormAttachment( 50, 0 );
        fdlTopics.top = new FormAttachment( wConsumerId, 10 );
        wlTopic.setLayoutData( fdlTopics );

        wTopic = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wTopic );
        wTopic.addModifyListener( lsMod );
        FormData fdTopic = new FormData();
        fdTopic.left = new FormAttachment( 0, 0 );
        fdTopic.right = new FormAttachment( 0, 363 );
        fdTopic.top = new FormAttachment( wlTopic, 5 );
        wTopic.setLayoutData( fdTopic );

        Label wlTags = new Label( wSetupComp, SWT.LEFT );
        props.setLook( wlTags );
        wlTags.setText( BaseMessages.getString( PKG, "RocketMQConsumerDialog.Tags") );
        FormData fdlTag = new FormData();
        fdlTag.left = new FormAttachment( 0, 0 );
        fdlTag.right = new FormAttachment( 50, 0 );
        fdlTag.top = new FormAttachment( wTopic, 10 );
        wlTags.setLayoutData( fdlTag );

        // Put last so it expands with the dialog. Anchoring itself to QOS Label and the Topics Label
        buildTagsTable( wSetupComp, wlTags, null );
    }

    private void buildTagsTable( Composite parentWidget, Control controlAbove, Control controlBelow ) {
        ColumnInfo[] columns = new ColumnInfo[] {
                new ColumnInfo( BaseMessages.getString( PKG, "RocketMQConsumerDialog.TagHeading"),
                        ColumnInfo.COLUMN_TYPE_TEXT, new String[ 1 ], false )
        };

        columns[ 0 ].setUsingVariables( true );

        int tagsCount = rocketMQMeta.getTags().size();

        wTagsTable = new TableView(
                transMeta,
                parentWidget,
                SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
                columns,
                tagsCount,
                false,
                lsMod,
                props,
                false
        );

        wTagsTable.setSortable( false );
        wTagsTable.getTable().addListener( SWT.Resize, event -> {
            Table table = (Table) event.widget;
            table.getColumn( 1 ).setWidth( 330 );
        } );

        FormData fdData = new FormData();
        fdData.left = new FormAttachment( 0, 0 );
        fdData.top = new FormAttachment( controlAbove, 5 );
        fdData.right = new FormAttachment( 0, 350 );
//        fdData.bottom = new FormAttachment( controlBelow, -10 );
        fdData.bottom = new FormAttachment( 100, 0 );

        // resize the columns to fit the data in them
        stream( wTagsTable.getTable().getColumns() ).forEach( column -> {
            if ( column.getWidth() > 0 ) {
                // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
                column.setWidth( 120 );
            }
        } );

        wTagsTable.setLayoutData( fdData );
    }


    @Override protected void createAdditionalTabs() {
        // Set the height so the topics table has approximately 5 rows
        shell.setMinimumSize( 527, 600 );
        securityLayout = new RocketMQDialogSecurityLayout(
                props, wTabFolder, rocketMQMeta.getAccessKey(), rocketMQMeta.getSecretKey(), lsMod, transMeta,
                rocketMQMeta.getSslConfig(), rocketMQMeta.isUseSsl() );
        securityLayout.buildSecurityTab();
        buildFieldsTab();
        optionsLayout = new RocketMQDialogOptionsLayout( props, wTabFolder, lsMod, transMeta, rocketMQMeta.retrieveOptions() );
        optionsLayout.buildTab();
    }


    @Override protected String[] getFieldNames() {
        return stream( fieldsTable.getTable().getItems() ).map( row -> row.getText( 2 ) ).toArray( String[]::new );
    }

    @Override protected int[] getFieldTypes() {
        return stream( fieldsTable.getTable().getItems() )
                .mapToInt( row -> ValueMetaFactory.getIdForValueMeta( row.getText( 3 ) ) ).toArray();
    }




    private void buildFieldsTab() {
        CTabItem wFieldsTab = new CTabItem( wTabFolder, SWT.NONE, 3 );
        wFieldsTab.setText( BaseMessages.getString( PKG, "RocketMQConsumerDialog.FieldsTab") );

        Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
        props.setLook( wFieldsComp );
        FormLayout fieldsLayout = new FormLayout();
        fieldsLayout.marginHeight = 15;
        fieldsLayout.marginWidth = 15;
        wFieldsComp.setLayout( fieldsLayout );

        FormData fieldsFormData = new FormData();
        fieldsFormData.left = new FormAttachment( 0, 0 );
        fieldsFormData.top = new FormAttachment( wFieldsComp, 0 );
        fieldsFormData.right = new FormAttachment( 100, 0 );
        fieldsFormData.bottom = new FormAttachment( 100, 0 );
        wFieldsComp.setLayoutData( fieldsFormData );

        buildFieldTable( wFieldsComp, wFieldsComp );

        wFieldsComp.layout();
        wFieldsTab.setControl( wFieldsComp );
    }

    private void buildFieldTable( Composite parentWidget, Control relativePosition ) {
        ColumnInfo[] columns = getFieldColumns();

        fieldsTable = new TableView(
                transMeta,
                parentWidget,
                SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
                columns,
                4,
                true,
                lsMod,
                props,
                false
        );

        fieldsTable.setSortable( false );
        fieldsTable.getTable().addListener( SWT.Resize, event -> {
            Table table = (Table) event.widget;
            table.getColumn( 1 ).setWidth( 147 );
            table.getColumn( 2 ).setWidth( 147 );
            table.getColumn( 3 ).setWidth( 147 );
        } );

        populateFieldData();

        FormData fdData = new FormData();
        fdData.left = new FormAttachment( 0, 0 );
        fdData.top = new FormAttachment( relativePosition, 5 );
        fdData.right = new FormAttachment( 100, 0 );

        // resize the columns to fit the data in them
        stream( fieldsTable.getTable().getColumns() ).forEach( column -> {
            if ( column.getWidth() > 0 ) {
                // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
                column.setWidth( 120 );
            }
        } );

        // don't let any rows get deleted or added (this does not affect the read-only state of the cells)
        fieldsTable.setReadonly( true );
        fieldsTable.setLayoutData( fdData );
    }

    private void populateFieldData() {
        TableItem messageIdItem = fieldsTable.getTable().getItem( 0 );
        messageIdItem.setText( 1, BaseMessages.getString( PKG, "RocketMQConsumerDialog.InputName.MessageID") );
        messageIdItem.setText( 2, rocketMQMeta.getMsgIdOutputName() );
        messageIdItem.setText( 3, "String" );

        TableItem topicItem = fieldsTable.getTable().getItem( 1 );
        topicItem.setText( 1, BaseMessages.getString( PKG, "RocketMQConsumerDialog.InputName.Topic") );
        topicItem.setText( 2, rocketMQMeta.getTopicOutputName() );
        topicItem.setText( 3, "String" );

        TableItem tagItem = fieldsTable.getTable().getItem( 2 );
        tagItem.setText( 1, BaseMessages.getString( PKG, "RocketMQConsumerDialog.InputName.Tag") );
        tagItem.setText( 2, rocketMQMeta.getTagOutputName() );
        tagItem.setText( 3, "String" );

        TableItem bodyItem = fieldsTable.getTable().getItem( 3 );
        bodyItem.setText( 1, BaseMessages.getString( PKG, "RocketMQConsumerDialog.InputName.Body") );
        bodyItem.setText( 2, rocketMQMeta.getBodyOutputName() );
        bodyItem.setText( 3, "String" );
    }

    private ColumnInfo[] getFieldColumns() {
        ColumnInfo referenceName = new ColumnInfo( BaseMessages.getString( PKG, "RocketMQConsumerDialog.Column.Ref"),
                ColumnInfo.COLUMN_TYPE_TEXT, false, true );

        ColumnInfo name = new ColumnInfo( BaseMessages.getString( PKG, "RocketMQConsumerDialog.Column.Name"),
                ColumnInfo.COLUMN_TYPE_TEXT, false, false );

        ColumnInfo type = new ColumnInfo( BaseMessages.getString( PKG, "RocketMQConsumerDialog.Column.Type"),
                ColumnInfo.COLUMN_TYPE_TEXT, false, true );

        return new ColumnInfo[] { referenceName, name, type };
    }


    @Override
    protected void additionalOks( BaseStreamStepMeta meta ) {
        rocketMQMeta.setONSAddr( wConnection.getText() );
        rocketMQMeta.setTopic( wTopic.getText() );
        rocketMQMeta.setConsumerId( wConsumerId.getText());
        rocketMQMeta.setTags( stream( wTagsTable.getTable().getItems() )
                .map( item -> item.getText( 1 ) )
                .filter( t -> !"".equals( t ) )
                .distinct()
                .collect( Collectors.toList() ) );
        rocketMQMeta.setMsgIdOutputName( fieldsTable.getTable().getItem( 0 ).getText( 2 ) );
        rocketMQMeta.setTopicOutputName( fieldsTable.getTable().getItem( 1 ).getText( 2 ) );
        rocketMQMeta.setTagOutputName( fieldsTable.getTable().getItem( 2 ).getText( 2 ) );
        rocketMQMeta.setBodyOutputName( fieldsTable.getTable().getItem( 3 ).getText( 2 ) );
        rocketMQMeta.setAccessKey( securityLayout.accessKey() );
        rocketMQMeta.setSecretKey( securityLayout.secretKey() );
        rocketMQMeta.setUseSsl( securityLayout.useSsl() );
        rocketMQMeta.setSslConfig( securityLayout.sslConfig() );

        optionsLayout.retrieveOptions().stream()
                .forEach( option -> {
                    switch ( option.getKey() ) {
                        case RocketMQConstants.SUSPEND_TIME_MILLIS:
                            rocketMQMeta.setSuspendTimeMillis( option.getValue() );
                            break;
                        case RocketMQConstants.MAX_RECONSUME_TIMES:
                            rocketMQMeta.setMaxReconsumeTimes( option.getValue() );
                            break;
                        case RocketMQConstants.CONSUME_TIMEOUT:
                            rocketMQMeta.setConsumeTimeout( option.getValue() );
                            break;
                    }
                } );
    }

}
