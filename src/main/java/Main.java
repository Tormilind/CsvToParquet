import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

@Slf4j
public class Main {
    @Option(name = "--csvFile", required = true, usage = "Csv file to convert")
    private String csvFile = "";
    @Option(name = "--schema", required = true, usage = "Schema to convert to")
    private String schemaFile = "";

    private int written = 0;

    public static void main(String[] args) {
        Main converter = new Main();
        CmdLineParser parser = new CmdLineParser(converter);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            parser.printUsage(System.err);
            System.exit(1);
        }
        converter.run();
    }

    public void run() {
        CsvInputParser csvParser = new CsvInputParser();
        csvParser.beginParsing(new File(csvFile));
        ParquetWriter<GenericData.Record> parquetWriter = null;
        Path newFilePath = new Path(csvFile + ".parquet");
        int blockSize = 1024 * 1024;
        int pageSize = 1024 * 1024 * 16;

        JSONObject mainjson = getSchemaFromJsonFile(schemaFile);
        if(mainjson == null){
            log.info("Error reading the schema file. Please check the JSON file");
        }
        else {
            Schema avroSchema = new Schema.Parser().parse(mainjson.toString());
            try {
                parquetWriter = AvroParquetWriter
                        .<GenericData.Record>builder(newFilePath)
                        .withSchema(avroSchema)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withRowGroupSize(blockSize)
                        .withPageSize(pageSize)
                        .build();

                InputRecord record;

                try{
                    while ((record = csvParser.parseNextRecord()) != null) {
                        GenericData.Record toWrite;
                        toWrite = convertToParquetRecord(avroSchema, record, csvParser.getHeader());
                        parquetWriter.write(toWrite);
                        written += 1;
                    }
                }
                catch(NullPointerException e){
                    log.info("Error. Encountered an empty file or a file with only a header row.");
                }
                finally{
                    csvParser.stopParsing();
                    System.out.println("Total of " + written + " records written into parquetfile");
                }


            } catch (IOException e) {
                log.info(e.getMessage());
            } finally {
                if (parquetWriter != null) {
                    try {
                        parquetWriter.close();
                    } catch (IOException e) {
                        log.info(e.getMessage());
                    }
                }
            }
        }
    }

    private JSONObject getSchemaFromJsonFile(String schemaFile) {
        JSONParser parser = new JSONParser();
        JSONObject jsonSchema = null;
        try {
            Object obj = parser.parse(new FileReader(schemaFile));

            // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
            jsonSchema = (JSONObject) obj;
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        return jsonSchema;
    }

    private Schema.Type getSchemaType(Schema subschema) {
        if (subschema.getType() == Schema.Type.UNION) {
            return subschema.getTypes().get(1).getType();
        } else {
            return subschema.getType();
        }
    }

    private GenericData.Record convertToParquetRecord(Schema schema, InputRecord record, String[] headers){
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        for(Schema.Field field: schema.getFields()){
            Schema.Type schemaType = getSchemaType(field.schema());
            switch (schemaType){
                case LONG:
                    convertLong(record, recordBuilder, field);
                    break;
                case DOUBLE:
                    convertDouble(record, recordBuilder, field);
                    break;
                case FLOAT:
                    convertFloat(record, recordBuilder, field);
                    break;
                case STRING:
                    convertString(record, recordBuilder, field);
                    break;
                default:
                    log.info("Unknown data type in schema.");
                    break;
            }
        }
        return recordBuilder.build();
    }

    private void convertLong(InputRecord record, GenericRecordBuilder recordBuilder, Schema.Field field) {
        String answer;
        String fieldName = field.name();
        answer = record.getString(fieldName);
        try {
            Long numberAnswer = Long.parseLong(answer);
            recordBuilder.set(field, numberAnswer);
        }
        catch(NumberFormatException e){
            recordBuilder.set(field, null);
        }
        catch(NullPointerException e){
            recordBuilder.set(field, null);
        }
    }
    private void convertDouble(InputRecord record, GenericRecordBuilder recordBuilder, Schema.Field field) {
        String answer;
        String fieldName = field.name();
        answer = record.getString(fieldName);
        try {
            Double numberAnswer = Double.parseDouble(answer);
            recordBuilder.set(field, numberAnswer);
        }
        catch(NumberFormatException e){
            recordBuilder.set(field, null);
        }
        catch(NullPointerException e){
            recordBuilder.set(field, null);
        }
    }
    private void convertFloat(InputRecord record, GenericRecordBuilder recordBuilder, Schema.Field field) {
        String answer;
        String fieldName = field.name();
        answer = record.getString(fieldName);
        try {
            Float numberAnswer = Float.parseFloat(answer);
            recordBuilder.set(field, numberAnswer);
        }
        catch(NumberFormatException e){
            recordBuilder.set(field, null);
        }
        catch(NullPointerException e){
            recordBuilder.set(field, null);
        }
    }
    private void convertString(InputRecord record, GenericRecordBuilder recordBuilder, Schema.Field field) {
        String answer;
        String fieldName = field.name();
        answer = record.getString(fieldName);
        if(answer.equals("")){
            recordBuilder.set(field, null);
        }
        else{
            recordBuilder.set(field, answer);
        }
    }
}
