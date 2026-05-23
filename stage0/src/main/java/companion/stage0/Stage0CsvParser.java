package companion.stage0;

import org.apache.hadoop.io.Text;

final class Stage0CsvParser {

    private Stage0CsvParser() {}

    static ParseResult parseVid(Text line) {
        String s = line.toString();
        int firstComma = s.indexOf(',');
        if (firstComma < 0) {
            return ParseResult.fail();
        }
        int vid = parseNonNegativeInt(s, 0, firstComma);
        if (vid < 0) {
            return ParseResult.fail();
        }
        return ParseResult.ok(vid, 0, 0);
    }

    static ParseResult parseRecord(Text line, int t0) {
        String s = line.toString();
        int firstComma = s.indexOf(',');
        if (firstComma < 0) {
            return ParseResult.fail();
        }
        int secondComma = s.indexOf(',', firstComma + 1);
        if (secondComma < 0 || s.indexOf(',', secondComma + 1) >= 0) {
            return ParseResult.fail();
        }

        int vid = parseNonNegativeInt(s, 0, firstComma);
        int loc = parseNonNegativeInt(s, firstComma + 1, secondComma);
        long ts = parseNonNegativeLong(s, secondComma + 1, s.length());
        if (vid < 0 || loc < 0 || ts < 0) {
            return ParseResult.fail();
        }

        long tNormLong = ts - t0;
        if (tNormLong < 0 || tNormLong > Integer.MAX_VALUE) {
            return ParseResult.fail();
        }
        return ParseResult.ok(vid, loc, (int) tNormLong);
    }

    private static int parseNonNegativeInt(String s, int start, int end) {
        long value = parseNonNegativeLong(s, start, end);
        if (value < 0 || value > Integer.MAX_VALUE) {
            return -1;
        }
        return (int) value;
    }

    private static long parseNonNegativeLong(String s, int start, int end) {
        while (start < end && Character.isWhitespace(s.charAt(start))) {
            start++;
        }
        while (end > start && Character.isWhitespace(s.charAt(end - 1))) {
            end--;
        }
        if (start >= end) {
            return -1L;
        }

        long value = 0L;
        for (int i = start; i < end; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                return -1L;
            }
            value = value * 10L + (c - '0');
            if (value < 0L) {
                return -1L;
            }
        }
        return value;
    }

    static final class ParseResult {
        private static final ParseResult FAIL = new ParseResult(false, 0, 0, 0);

        private final boolean ok;
        private final int vid;
        private final int loc;
        private final int tNorm;

        private ParseResult(boolean ok, int vid, int loc, int tNorm) {
            this.ok = ok;
            this.vid = vid;
            this.loc = loc;
            this.tNorm = tNorm;
        }

        static ParseResult ok(int vid, int loc, int tNorm) {
            return new ParseResult(true, vid, loc, tNorm);
        }

        static ParseResult fail() {
            return FAIL;
        }

        boolean isOk() {
            return ok;
        }

        int vid() {
            return vid;
        }

        int loc() {
            return loc;
        }

        int tNorm() {
            return tNorm;
        }
    }
}
