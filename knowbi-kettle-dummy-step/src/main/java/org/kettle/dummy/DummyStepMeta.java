package org.kettle.dummy;

import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.*;
import org.pentaho.di.core.annotations.*;
import org.pentaho.di.core.database.*;
import org.pentaho.di.core.exception.*;
import org.pentaho.di.core.row.*;
import org.pentaho.di.core.row.value.*;
import org.pentaho.di.core.variables.*;
import org.pentaho.di.core.xml.*;
import org.pentaho.di.repository.*;
import org.pentaho.di.trans.*;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.*;

import java.util.List;
import java.util.*;

/*
 * Created on 02-jun-2003
 *
 */

@Step( id = "DummySample",
        image = "ui/images/deprecated.svg",
        i18nPackageName = "be.ibridge.kettle.dummy",
        name = "Dummy.Step.Name",
        description = "Dummy.Step.Description",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Flow"
//        suggestion = "DummyStep.Step.SuggestedStep"
        )
public class DummyStepMeta extends BaseStepMeta implements StepMetaInterface {
    private ValueMetaAndData value;

    public DummyStepMeta() {
        super(); // allocate BaseStepInfo
    }

    /**
     * @return Returns the value.
     */
    public ValueMetaAndData getValue() {
        return value;
    }

    /**
     * @param value The value to set.
     */
    public void setValue( ValueMetaAndData value ) {
        this.value = value;
    }

    @Override
    public String getXML() throws KettleException {
        String retval = "";

        retval += "    <values>" + Const.CR;
        if ( value != null ) {
            retval += value.getXML();
        }
        retval += "      </values>" + Const.CR;

        return retval;
    }

    @Override
    public void getFields( RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore ) {
        if ( value != null ) {
            ValueMetaInterface v = value.getValueMeta();
            v.setOrigin( origin );

            r.addValueMeta( v );
        }
    }

    @Override
    public Object clone() {
        Object retval = super.clone();
        return retval;
    }

    @Override
    public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        try {
            value = new ValueMetaAndData();

            Node valnode = XMLHandler.getSubNode( stepnode, "values", "value" );
            if ( valnode != null ) {
                System.out.println( "reading value in " + valnode );
                value.loadXML( valnode );
            }
        } catch ( Exception e ) {
            throw new KettleXMLException( "Unable to read step info from XML node", e );
        }
    }

    @Override
    public void setDefault() {
        value = new ValueMetaAndData( new ValueMetaNumber( "valuename" ), new Double( 123.456 ) );
        value.getValueMeta().setLength( 12 );
        value.getValueMeta().setPrecision( 4 );
    }

    @Override
    public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
        try {
            String name = rep.getStepAttributeString( id_step, 0, "value_name" );
            String typedesc = rep.getStepAttributeString( id_step, 0, "value_type" );
            String text = rep.getStepAttributeString( id_step, 0, "value_text" );
            boolean isnull = rep.getStepAttributeBoolean( id_step, 0, "value_null" );
            int length = (int) rep.getStepAttributeInteger( id_step, 0, "value_length" );
            int precision = (int) rep.getStepAttributeInteger( id_step, 0, "value_precision" );

            int type = ValueMetaFactory.getIdForValueMeta( typedesc );
            value = new ValueMetaAndData( new ValueMeta( name, type ), null );
            value.getValueMeta().setLength( length );
            value.getValueMeta().setPrecision( precision );

            if ( isnull ) {
                value.setValueData( null );
            } else {
                ValueMetaInterface stringMeta = new ValueMetaString( name );
                if ( type != ValueMetaInterface.TYPE_STRING ) {
                    text = Const.trim( text );
                }
                value.setValueData( value.getValueMeta().convertData( stringMeta, text ) );
            }
        } catch ( KettleDatabaseException dbe ) {
            throw new KettleException( "error reading step with id_step=" + id_step + " from the repository", dbe );
        } catch ( Exception e ) {
            throw new KettleException( "Unexpected error reading step with id_step=" + id_step + " from the repository", e );
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        try {
            rep.saveStepAttribute( id_transformation, id_step, "value_name", value.getValueMeta().getName() );
            rep.saveStepAttribute( id_transformation, id_step, 0, "value_type", value.getValueMeta().getTypeDesc() );
            rep.saveStepAttribute( id_transformation, id_step, 0, "value_text", value.getValueMeta().getString( value.getValueData() ) );
            rep.saveStepAttribute( id_transformation, id_step, 0, "value_null", value.getValueMeta().isNull( value.getValueData() ) );
            rep.saveStepAttribute( id_transformation, id_step, 0, "value_length", value.getValueMeta().getLength() );
            rep.saveStepAttribute( id_transformation, id_step, 0, "value_precision", value.getValueMeta().getPrecision() );
        } catch ( KettleDatabaseException dbe ) {
            throw new KettleException( "Unable to save step information to the repository, id_step=" + id_step, dbe );
        }
    }

    @Override
    public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository, IMetaStore metaStore ) {
        CheckResult cr;
        if ( prev == null || prev.size() == 0 ) {
            cr = new CheckResult( CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!", stepMeta );
            remarks.add( cr );
        } else {
            cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is connected to previous one, receiving " + prev.size() + " fields", stepMeta );
            remarks.add( cr );
        }

        // See if we have input streams leading to this step!
        if ( input.length > 0 ) {
            cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta );
            remarks.add( cr );
        } else {
            cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta );
            remarks.add( cr );
        }
    }

    public StepDialogInterface getDialog( Shell shell, StepMetaInterface meta, TransMeta transMeta, String name ) {
        return new DummyStepDialog( shell, meta, transMeta, name );
    }

    @Override
    public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp ) {
        return new DummyStep( stepMeta, stepDataInterface, cnr, transMeta, disp );
    }

    @Override
    public StepDataInterface getStepData() {
        return new DummyStepData();
    }
}