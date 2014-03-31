package org.apache.hadoop.hive.serde2.lazy.objectinspector;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.hadoop.hive.serde2.lazy.CassandraLazyValidator;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.AbstractPrimitiveLazyObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * Forces everything to strings using the cassandra validator
 * CassandraValidatorObjectInspector.
 *
 */
public class CassandraValidatorObjectInspector
        extends AbstractPrimitiveLazyObjectInspector<Text>
        implements StringObjectInspector {

    private final AbstractType validator;

    public CassandraValidatorObjectInspector(AbstractType validator) {
        super(PrimitiveObjectInspectorUtils.stringTypeEntry);
        this.validator = validator;
    }

    @Override
    public String getTypeName() {
        return PrimitiveObjectInspectorUtils.stringTypeEntry.typeName;
    }

    @Override
    public Category getCategory() {
        return Category.PRIMITIVE;
    }

    public AbstractType getValidatorType() {
        return validator;
    }

    @Override
    public String getPrimitiveJavaObject(Object o) {
        return o == null ? null : ((CassandraLazyValidator) o).getWritableObject().toString();
    }

    @Override
    public Object copyObject(Object o) {
        return o == null ? null : new CassandraLazyValidator((CassandraLazyValidator) o);
    }
}
