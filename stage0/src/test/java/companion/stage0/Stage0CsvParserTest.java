package companion.stage0;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Stage0CsvParserTest {

    @Test
    public void parsesVidWithoutSplittingWholeLine() {
        Stage0CsvParser.ParseResult parsed = Stage0CsvParser.parseVid(new Text(" 42,7,1420041602 "));

        assertTrue(parsed.isOk());
        assertEquals(42, parsed.vid());
    }

    @Test
    public void parsesFullRecordAndNormalizesTimestamp() {
        Stage0CsvParser.ParseResult parsed = Stage0CsvParser.parseRecord(
                new Text("42,7,1420041602"), 1420041600);

        assertTrue(parsed.isOk());
        assertEquals(42, parsed.vid());
        assertEquals(7, parsed.loc());
        assertEquals(2, parsed.tNorm());
    }

    @Test
    public void rejectsMalformedRows() {
        assertFalse(Stage0CsvParser.parseRecord(new Text("42,7"), 1420041600).isOk());
        assertFalse(Stage0CsvParser.parseRecord(new Text("42,,1420041602"), 1420041600).isOk());
        assertFalse(Stage0CsvParser.parseRecord(new Text("x,7,1420041602"), 1420041600).isOk());
        assertFalse(Stage0CsvParser.parseRecord(new Text("42,7,1420041599"), 1420041600).isOk());
        assertFalse(Stage0CsvParser.parseRecord(new Text("42,7,9999999999999"), 1420041600).isOk());
    }
}
