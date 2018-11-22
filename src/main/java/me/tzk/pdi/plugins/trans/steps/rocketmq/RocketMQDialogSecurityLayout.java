package me.tzk.pdi.plugins.trans.steps.rocketmq;

import com.google.common.collect.Lists;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;
import static java.util.Collections.sort;
import static org.pentaho.di.ui.trans.step.BaseStreamingDialog.INPUT_WIDTH;

public class RocketMQDialogSecurityLayout {
    private static Class<?> PKG = RocketMQDialogSecurityLayout.class;

    private final PropsUI props;
    private final CTabFolder wTabFolder;
    private final ModifyListener lsMod;
    private final TransMeta transMeta;
    private final String accessKey;
    private final String secretKey;
    private final Map<String, String> sslConfig;
    private final boolean sslEnabled;

    private TableView sslTable;
    private Button wUseSSL;
    private TextVar wAccessKey;
    private TextVar wSecretKey;

    RocketMQDialogSecurityLayout(
            PropsUI props, CTabFolder wTabFolder, String accessKey,
            String secretKey, ModifyListener lsMod, TransMeta transMeta,
            Map<String, String> sslConfig, boolean sslEnabled ) {
        checkNotNull( props );
        checkNotNull( wTabFolder );
        checkNotNull( lsMod );
        checkNotNull( transMeta );

        this.props = props;
        this.wTabFolder = wTabFolder;
        this.lsMod = lsMod;
        this.transMeta = transMeta;
        this.sslEnabled = sslEnabled;
        this.sslConfig = Optional.ofNullable( sslConfig ).orElse( emptyMap() );
        this.accessKey = nullToEmpty( accessKey );
        this.secretKey = nullToEmpty( secretKey );
    }

    String accessKey() {
        return wAccessKey.getText();
    }

    String secretKey() {
        return wSecretKey.getText();
    }

    Map<String, String> sslConfig() {
        return tableToMap( sslTable );
    }

    boolean useSsl() {
        return wUseSSL.getSelection();
    }

    private Map<String, String> tableToMap( TableView table ) {
        return IntStream.range( 0, table.getItemCount() )
                .mapToObj( table::getItem )
                .collect( Collectors.toMap(strArray -> strArray[ 0 ], strArray -> strArray[ 1 ] ) );
    }

    void buildSecurityTab() {
        CTabItem wSecurityTab = new CTabItem( wTabFolder, SWT.NONE, 1 );
        wSecurityTab.setText( BaseMessages.getString( PKG, "RocketMQDialog.Security.Tab") );

        Composite wSecurityComp = new Composite( wTabFolder, SWT.NONE );
        props.setLook( wSecurityComp );
        FormLayout securityLayout = new FormLayout();
        securityLayout.marginHeight = 15;
        securityLayout.marginWidth = 15;
        wSecurityComp.setLayout( securityLayout );

        // Authentication group
        Group wAuthenticationGroup = new Group( wSecurityComp, SWT.SHADOW_ETCHED_IN );
        props.setLook( wAuthenticationGroup );
        wAuthenticationGroup.setText( BaseMessages.getString( PKG, "RocketMQDialog.Security.Authentication") );
        FormLayout flAuthentication = new FormLayout();
        flAuthentication.marginHeight = 15;
        flAuthentication.marginWidth = 15;
        wAuthenticationGroup.setLayout( flAuthentication );

        FormData fdAuthenticationGroup = new FormData();
        fdAuthenticationGroup.left = new FormAttachment( 0, 0 );
        fdAuthenticationGroup.top = new FormAttachment( 0, 0 );
        fdAuthenticationGroup.right = new FormAttachment( 100, 0 );
        fdAuthenticationGroup.width = INPUT_WIDTH;
        wAuthenticationGroup.setLayoutData( fdAuthenticationGroup );

        Label wlAccessKey = new Label( wAuthenticationGroup, SWT.LEFT );
        props.setLook( wlAccessKey );
        wlAccessKey.setText( BaseMessages.getString( PKG, "RocketMQDialog.Security.AccessKey") );
        FormData fdlAccessKey = new FormData();
        fdlAccessKey.left = new FormAttachment( 0, 0 );
        fdlAccessKey.top = new FormAttachment( 0, 0 );
        fdlAccessKey.right = new FormAttachment( 0, INPUT_WIDTH );
        wlAccessKey.setLayoutData( fdlAccessKey );

        wAccessKey = new TextVar( transMeta, wAuthenticationGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );

        props.setLook( wAccessKey );
        wAccessKey.addModifyListener( lsMod );
        FormData fdAccessKey = new FormData();
        fdAccessKey.left = new FormAttachment( 0, 0 );
        fdAccessKey.top = new FormAttachment( wlAccessKey, 5 );
        fdAccessKey.right = new FormAttachment( 0, INPUT_WIDTH );
        wAccessKey.setLayoutData( fdAccessKey );

        Label wlSecretKey = new Label( wAuthenticationGroup, SWT.LEFT );
        props.setLook( wlSecretKey );
        wlSecretKey.setText( BaseMessages.getString( PKG, "RocketMQDialog.Security.SecretKey") );
        FormData fdlSecretKey = new FormData();
        fdlSecretKey.left = new FormAttachment( 0, 0 );
        fdlSecretKey.top = new FormAttachment( wAccessKey, 10 );
        fdlSecretKey.right = new FormAttachment( 0, INPUT_WIDTH );
        wlSecretKey.setLayoutData( fdlSecretKey );

        wSecretKey = new TextVar( transMeta, wAuthenticationGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wSecretKey );
        wSecretKey.addModifyListener( lsMod );
        FormData fdSecretKey = new FormData();
        fdSecretKey.left = new FormAttachment( 0, 0 );
        fdSecretKey.top = new FormAttachment( wlSecretKey, 5 );
        fdSecretKey.right = new FormAttachment( 0, INPUT_WIDTH );
        wSecretKey.setLayoutData( fdSecretKey );

        wUseSSL = new Button( wSecurityComp, SWT.CHECK );
        wUseSSL.setText( BaseMessages.getString( PKG, "RocketMQDialog.Security.UseSSL") );
        props.setLook( wUseSSL );
        FormData fdUseSSL = new FormData();
        fdUseSSL.top = new FormAttachment( wAuthenticationGroup, 15 );
        fdUseSSL.left = new FormAttachment( 0, 0 );
        wUseSSL.setLayoutData( fdUseSSL );
        wUseSSL.addSelectionListener( new SelectionListener() {
            @Override public void widgetSelected( SelectionEvent selectionEvent ) {
                boolean selection = ( (Button) selectionEvent.getSource() ).getSelection();
                sslTable.setEnabled( selection );
                sslTable.table.setEnabled( selection );
            }

            @Override public void widgetDefaultSelected( SelectionEvent selectionEvent ) {
                boolean selection = ( (Button) selectionEvent.getSource() ).getSelection();
                sslTable.setEnabled( selection );
                sslTable.table.setEnabled( selection );
            }
        } );

        Label wlSSLProperties = new Label( wSecurityComp, SWT.LEFT );
        wlSSLProperties.setText( BaseMessages.getString( PKG, "RocketMQDialog.Security.SSLProperties") );
        props.setLook( wlSSLProperties );
        FormData fdlSSLProperties = new FormData();
        fdlSSLProperties.top = new FormAttachment( wUseSSL, 10 );
        fdlSSLProperties.left = new FormAttachment( 0, 0 );
        wlSSLProperties.setLayoutData( fdlSSLProperties );

        FormData fdSecurityComp = new FormData();
        fdSecurityComp.left = new FormAttachment( 0, 0 );
        fdSecurityComp.top = new FormAttachment( 0, 0 );
        fdSecurityComp.right = new FormAttachment( 100, 0 );
        fdSecurityComp.bottom = new FormAttachment( 100, 0 );
        wSecurityComp.setLayoutData( fdSecurityComp );

        buildSSLTable( wSecurityComp, wlSSLProperties );

        wSecurityComp.layout();
        wSecurityTab.setControl( wSecurityComp );
    }


    void setUIText() {
        wUseSSL.setSelection( sslEnabled );
        sslTable.setEnabled( sslEnabled );
        sslTable.table.setEnabled( sslEnabled );

        sslTable.table.select( 0 );
        sslTable.table.showSelection();

        wAccessKey.setText( accessKey );
        wSecretKey.setText( secretKey );
    }

    private void buildSSLTable( Composite parentWidget, Control relativePosition ) {
        ColumnInfo[] columns = getSSLColumns();

        sslTable = new TableView(
                transMeta,
                parentWidget,
                SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
                columns,
                0,  // num of starting rows (will be added later)
                false,
                lsMod,
                props,
                false
        );

        sslTable.setSortable( false );
        sslTable.getTable().addListener( SWT.Resize, event -> {
            Table table = (Table) event.widget;
            table.getColumn( 1 ).setWidth( 200 );
            table.getColumn( 2 ).setWidth( 200 );
        } );

        populateSSLData();

        FormData fdData = new FormData();
        fdData.left = new FormAttachment( 0, 0 );
        fdData.top = new FormAttachment( relativePosition, 5 );
        fdData.bottom = new FormAttachment( 100, 0 );
        fdData.width = INPUT_WIDTH + 80;

        // resize the columns to fit the data in them
        stream( sslTable.getTable().getColumns() ).forEach( column -> {
            if ( column.getWidth() > 0 ) {
                // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
                column.setWidth( 200 );
            }
        } );

        sslTable.setLayoutData( fdData );
    }

    private ColumnInfo[] getSSLColumns() {
        ColumnInfo optionName = new ColumnInfo( BaseMessages.getString( PKG, "RocketMQDialog.Security.SSL.Column.Name"),
                ColumnInfo.COLUMN_TYPE_TEXT, false, false );

        ColumnInfo value = new ColumnInfo( BaseMessages.getString( PKG, "RocketMQDialog.Security.SSL.Column.Value"),
                ColumnInfo.COLUMN_TYPE_TEXT, false, false, 200 );
        value.setUsingVariables( true );

        return new ColumnInfo[] { optionName, value };
    }

    private void populateSSLData() {
        sslTable.getTable().removeAll();
        new TableItem( sslTable.getTable(), SWT.NONE );

        checkNotNull( sslTable.getItem( 0 ) );
        checkState( sslTable.getItem( 0 ).length == 2 );

        List<String> keys = Lists.newArrayList( sslConfig.keySet() );
        sort( keys );

        String firstKey = keys.remove( 0 );
        sslTable.getTable().getItem( 0 ).setText( 1, firstKey );
        sslTable.getTable().getItem( 0 ).setText( 2, sslConfig.get( firstKey ) );

        keys.stream().forEach( key -> sslTable.add( key, sslConfig.get( key ) ) );
    }


}
