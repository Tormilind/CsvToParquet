import com.univocity.parsers.common.record.Record;

public class CsvInputRecord implements InputRecord {
    Record record;

    public CsvInputRecord(Record record) {
        this.record = record;
    }

    @Override
    public String[] getValues() {
        return record.getValues();
    }

    @Override
    public String getString(String column) {
        return record.getString(column);
    }

    @Override
    public Long getLong(String column) {
        return record.getLong(column);
    }

    @Override
    public Double getDouble(String column) {
        return record.getDouble(column);
    }

    @Override
    public Float getFloat(String column) {
        return record.getFloat(column);
    }
}
