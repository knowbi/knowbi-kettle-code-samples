package org.kettle.dummy;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

public class DummyStep extends BaseStep implements StepInterface {
    private DummyStepData data;
    private DummyStepMeta meta;

    public DummyStep(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis ) {
        super( s, stepDataInterface, c, t, dis );
    }

    @Override
    public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
        meta = (DummyStepMeta) smi;
        data = (DummyStepData) sdi;

        Object[] r = getRow();    // get row, blocks when needed!
        if ( r == null ) { // no more input to be expected...
            setOutputDone();
            return false;
        }

        if ( first ) {
            first = false;

            data.outputRowMeta = getInputRowMeta().clone();
            meta.getFields( data.outputRowMeta, getStepname(), null, null, this );
        }

        Object extraValue = meta.getValue().getValueData();

        Object[] outputRow = RowDataUtil.addValueData( r, data.outputRowMeta.size() - 1, extraValue );

        putRow( data.outputRowMeta, outputRow );     // copy row to possible alternate rowset(s).

        if ( checkFeedback( getLinesRead() ) ) {
            logBasic( "Linenr " + getLinesRead() );  // Some basic logging every 5000 rows.
        }

        return true;
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi ) {
        meta = (DummyStepMeta) smi;
        data = (DummyStepData) sdi;

        return super.init( smi, sdi );
    }

    @Override
    public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
        meta = (DummyStepMeta) smi;
        data = (DummyStepData) sdi;

        super.dispose( smi, sdi );
    }

    //
    // Run is were the action happens!
    public void run() {
        logBasic( "Starting to run..." );
        try {
            while ( processRow( meta, data ) && !isStopped() ) {
                // Process rows
            }
        } catch ( Exception e ) {
            logError( "Unexpected error : " + e.toString() );
            logError( Const.getStackTracker( e ) );
            setErrors( 1 );
            stopAll();
        } finally {
            dispose( meta, data );
            logBasic( "Finished, processing " + getLinesRead() + " rows" );
            markStop();
        }
    }
}