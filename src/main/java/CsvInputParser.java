import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

@Slf4j
public class CsvInputParser implements InputParser {

    CsvParser parser;
    String[] header;

    public CsvInputParser() {
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        settings.setMaxColumns(1000);
        settings.setQuoteDetectionEnabled(true);
        settings.setMaxCharsPerColumn(-1);
        parser = new CsvParser(settings);
    }

    public CsvInputParser(String[] headers) {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setFormat(new CsvFormat());
        settings.getFormat().setLineSeparator("\n");
        settings.getFormat().setQuoteEscape('\\');
        settings.setMaxColumns(1000);
        settings.setMaxCharsPerColumn(-1);
        settings.setQuoteDetectionEnabled(true);
        settings.setHeaders(headers);
        parser = new CsvParser(settings);
        header = headers;
    }

    @Override
    public void beginParsing(File file) {
        try {
            if (file.getName().endsWith(".zip")) {
                parser.beginParsing(new ZipInputStream(new FileInputStream(file)));
                log.info("Using zip reader for {}", file);
            }
            else if(file.getName().endsWith(".gz")){
                parser.beginParsing(new GZIPInputStream(new FileInputStream(file)));
                log.info("Using gzip reader for {}", file);
            }
            else {
                log.info("Using csv reader for {}", file);
                parser.beginParsing(file);
            }
        }
        catch(FileNotFoundException e){
            log.info("Error finding the data file: " + file.getName());
        }
        catch(IOException e){
            log.info("Unknown error when parsing data file: " + file.getName());
        }
        if(header == null) {
            header = parser.parseNextRecord().getValues();
        }
    }

    @Override
    public String[] getHeader() {
        return header.clone();
    }

    @Override
    public InputRecord parseNextRecord() {
        Record record = parser.parseNextRecord();
        if (record == null) {
            return null;
        }
        return new CsvInputRecord(record);
    }

    @Override
    public void stopParsing() {
        parser.stopParsing();

    }
}
