package org.kettle.dummy;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class DummyStepData extends BaseStepData implements StepDataInterface {
    public RowMetaInterface outputRowMeta;

    public DummyStepData() {
        super();
    }
}