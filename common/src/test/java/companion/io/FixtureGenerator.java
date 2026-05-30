package companion.io;

import companion.conf.CompanionConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Single-process reference implementation of Stage 0/1/2 over tests/data/mini.csv.
 * Emits canonical golden fixtures under tests/data/fixtures/.
 *
 * <p>Outputs (committed to the repo):
 * <ul>
 *   <li>{@code filtered.seq}        — SequenceFile&lt;NullWritable, RecordWritable&gt;</li>
 *   <li>{@code pair_loc_slot.seq}   — SequenceFile&lt;PairKey, LocSlotWritable&gt;</li>
 *   <li>{@code companions.csv}      — text {@code vidA,vidB,count} sorted by (count desc, pair asc)</li>
 * </ul>
 *
 * <p>SequenceFile compression is {@link CompressionType#BLOCK} with the pure-Java
 * {@link DefaultCodec} (gzip). Full mini.csv expands {@code pair_loc_slot.seq} to ~80 MB
 * uncompressed; BLOCK+gzip brings it under 10 MB so the fixture stays committable. DefaultCodec
 * is in hadoop-common and needs no native library, keeping the generator byte-stable across
 * machines. Hadoop SequenceFile readers auto-detect the codec from the file header, so downstream
 * stage code reads these transparently regardless of production codec.
 *
 * <p>Re-run after any change to {@link RecordWritable}, {@link PairKey}, {@link LocSlotWritable},
 * or to the Stage 0/1/2 semantics declared in {@code docs/reference-semantics.md}.
 *
 * <p>Stage 1 here is the <b>ideal</b> semantics: group by {@code loc} only, sort by {@code t},
 * slide a {@code |Δt| <= delta.t} window, skip same-vid, attribute each pair to the LATER
 * record's slot ({@code slot = t / slot.size}). There is no {@code slot/2} partitioning and no
 * boundary loss. Production {@code Stage1Job} currently under-approximates this — see
 * {@code docs/stage1-boundary-gap.md}. The gap between this fixture and a real cluster run of
 * Stage 1 is the measured size of that defect (J1b will close it).
 *
 * <p>Input is the full {@code tests/data/mini.csv}; cluster_test.sh feeds the same full file to
 * stage0 so the upstream record set matches what is encoded here.
 */
public final class FixtureGenerator {

    private FixtureGenerator() {}

    public static void main(String[] args) throws IOException {
        File repoRoot = args.length > 0
            ? new File(args[0]).getCanonicalFile()
            : findRepoRoot();
        File miniCsv = new File(repoRoot, "tests/data/mini.csv");
        File outDir = new File(repoRoot, "tests/data/fixtures");
        if (!miniCsv.isFile()) {
            throw new IllegalStateException("not found: " + miniCsv);
        }
        if (!outDir.isDirectory()) {
            throw new IllegalStateException("not found: " + outDir);
        }

        Configuration conf = new Configuration();
        int t0 = CompanionConf.t0(conf);
        int deltaT = CompanionConf.deltaT(conf);
        int slotSize = CompanionConf.slotSize(conf);
        int kMin = CompanionConf.kMin(conf);

        log("config: t0=" + t0 + ", deltaT=" + deltaT
            + ", slotSize=" + slotSize + ", kMin=" + kMin);

        long t = System.currentTimeMillis();
        List<int[]> filtered = stage0(miniCsv, t0);
        log("stage0: kept " + filtered.size() + " records (" + ms(t) + " ms)");

        t = System.currentTimeMillis();
        writeFiltered(conf, filtered, new File(outDir, "filtered.seq"));
        log("wrote filtered.seq (" + ms(t) + " ms)");

        t = System.currentTimeMillis();
        List<long[]> witnesses = stage1(filtered, deltaT, slotSize);
        log("stage1: " + witnesses.size() + " pair witnesses (" + ms(t) + " ms)");

        t = System.currentTimeMillis();
        writePairLocSlot(conf, witnesses, new File(outDir, "pair_loc_slot.seq"));
        log("wrote pair_loc_slot.seq (" + ms(t) + " ms)");

        t = System.currentTimeMillis();
        List<long[]> companions = stage2(witnesses, kMin);
        log("stage2: " + companions.size() + " companion pairs surviving k.min=" + kMin
            + " (" + ms(t) + " ms)");

        t = System.currentTimeMillis();
        writeCompanions(companions, new File(outDir, "companions.csv"));
        log("wrote companions.csv (" + ms(t) + " ms)");
    }

    // ===== Stage 0: parse + frequency filter + t-normalize =====

    /**
     * Returns rows of {vid, loc, tNorm} for every line of mini.csv, keeping only vids with
     * frequency >= 2 across the full file.
     */
    static List<int[]> stage0(File miniCsv, int t0) throws IOException {
        Map<Integer, Integer> vidCount = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(miniCsv))) {
            String line;
            while ((line = br.readLine()) != null) {
                int[] parsed = parseRecord(line, t0);
                if (parsed == null) continue;
                vidCount.merge(parsed[0], 1, Integer::sum);
            }
        }
        List<int[]> kept = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(miniCsv))) {
            String line;
            while ((line = br.readLine()) != null) {
                int[] parsed = parseRecord(line, t0);
                if (parsed == null) continue;
                if (vidCount.getOrDefault(parsed[0], 0) < 2) continue;
                kept.add(parsed);
            }
        }
        return kept;
    }

    /**
     * Mirrors {@code stage0.Stage0CsvParser.parseRecord} byte-for-byte: requires exactly two
     * commas; non-negative {@code vid/loc/ts}; {@code 0 <= ts - t0 <= Integer.MAX_VALUE};
     * field-internal whitespace tolerated; no integer overflow. We can't share the production
     * parser directly — it's package-private in stage0 and common can't depend on stage0
     * (would invert the build graph). Any change here MUST mirror Stage0CsvParser.
     *
     * @return {vid, loc, tNorm} on success, else null
     */
    private static int[] parseRecord(String line, int t0) {
        if (line == null) return null;
        int firstComma = line.indexOf(',');
        if (firstComma < 0) return null;
        int secondComma = line.indexOf(',', firstComma + 1);
        if (secondComma < 0 || line.indexOf(',', secondComma + 1) >= 0) return null;
        long vid = parseNonNegativeLong(line, 0, firstComma);
        long loc = parseNonNegativeLong(line, firstComma + 1, secondComma);
        long ts = parseNonNegativeLong(line, secondComma + 1, line.length());
        if (vid < 0 || vid > Integer.MAX_VALUE
                || loc < 0 || loc > Integer.MAX_VALUE
                || ts < 0) {
            return null;
        }
        long tNorm = ts - t0;
        if (tNorm < 0 || tNorm > Integer.MAX_VALUE) return null;
        return new int[]{(int) vid, (int) loc, (int) tNorm};
    }

    private static long parseNonNegativeLong(String s, int start, int end) {
        while (start < end && Character.isWhitespace(s.charAt(start))) start++;
        while (end > start && Character.isWhitespace(s.charAt(end - 1))) end--;
        if (start >= end) return -1L;
        long value = 0L;
        for (int i = start; i < end; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') return -1L;
            value = value * 10L + (c - '0');
            if (value < 0L) return -1L;
        }
        return value;
    }

    // ===== Stage 1 (ideal): sliding-window pair emission per loc =====

    /**
     * Groups records by {@code loc} (no {@code slot/2} partitioning); within each loc runs a
     * sliding window pairing records with {@code |Δt| <= deltaT}. Each pair witness is attributed
     * to the LATER record's slot ({@code slot = t / slotSize}). Same-vid pairs are skipped.
     *
     * <p>This is the ideal reference. Production {@code Stage1Job} currently partitions on
     * {@code (loc, slot/2)} and loses {@code 2k+1 → 2k+2} cross-partition pairs — see
     * {@code docs/stage1-boundary-gap.md}.
     *
     * @return list of {vidA, vidB, loc, slot} with {@code vidA < vidB}
     */
    static List<long[]> stage1(List<int[]> records, int deltaT, int slotSize) {
        Map<Integer, List<int[]>> groups = new HashMap<>();
        for (int[] r : records) {
            groups.computeIfAbsent(r[1], k -> new ArrayList<>()).add(r);
        }

        List<long[]> witnesses = new ArrayList<>();
        ArrayDeque<int[]> window = new ArrayDeque<>();
        for (List<int[]> group : groups.values()) {
            group.sort((a, b) -> Integer.compare(a[2], b[2]));
            window.clear();
            for (int[] cur : group) {
                int curT = cur[2];
                int curVid = cur[0];
                int curLoc = cur[1];
                int curSlot = curT / slotSize;
                while (!window.isEmpty() && window.peekFirst()[2] < curT - deltaT) {
                    window.pollFirst();
                }
                for (int[] prev : window) {
                    if (prev[0] == curVid) continue;
                    int va = Math.min(prev[0], curVid);
                    int vb = Math.max(prev[0], curVid);
                    witnesses.add(new long[]{va, vb, curLoc, curSlot});
                }
                window.addLast(cur);
            }
        }
        return witnesses;
    }

    // ===== Stage 2: distinct (loc, slot) count per pair, threshold filter =====

    /**
     * Groups witnesses by PairKey, counts distinct {@code (loc, slot)} cells, retains pairs with
     * {@code count >= kMin}. Output sorted by (count desc, vidA asc, vidB asc) to match Stage 3
     * globally-sorted output order.
     *
     * @return list of {vidA, vidB, count}
     */
    static List<long[]> stage2(List<long[]> witnesses, int kMin) {
        Map<Long, Set<Long>> pairToCells = new HashMap<>();
        for (long[] w : witnesses) {
            long pair = (w[0] << 32) | (w[1] & 0xffffffffL);
            long cell = (w[2] << 32) | (w[3] & 0xffffffffL);
            pairToCells.computeIfAbsent(pair, k -> new HashSet<>()).add(cell);
        }
        List<long[]> out = new ArrayList<>();
        for (Map.Entry<Long, Set<Long>> e : pairToCells.entrySet()) {
            int count = e.getValue().size();
            if (count < kMin) continue;
            long pair = e.getKey();
            int vidA = (int) (pair >>> 32);
            int vidB = (int) pair;
            out.add(new long[]{vidA, vidB, count});
        }
        out.sort((a, b) -> {
            int c = Long.compare(b[2], a[2]);
            if (c != 0) return c;
            c = Long.compare(a[0], b[0]);
            if (c != 0) return c;
            return Long.compare(a[1], b[1]);
        });
        return out;
    }

    // ===== Output =====

    private static void writeFiltered(Configuration conf, List<int[]> records, File out)
            throws IOException {
        Path p = new Path(out.toURI());
        try (SequenceFile.Writer w = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(p),
                SequenceFile.Writer.keyClass(NullWritable.class),
                SequenceFile.Writer.valueClass(RecordWritable.class),
                SequenceFile.Writer.compression(CompressionType.BLOCK, new DefaultCodec()))) {
            RecordWritable v = new RecordWritable();
            for (int[] r : records) {
                v.set(r[0], r[1], r[2]);
                w.append(NullWritable.get(), v);
            }
        }
    }

    private static void writePairLocSlot(Configuration conf, List<long[]> witnesses, File out)
            throws IOException {
        // Canonical order: (vidA, vidB, loc, slot) ascending.
        witnesses.sort((a, b) -> {
            int c = Long.compare(a[0], b[0]);
            if (c != 0) return c;
            c = Long.compare(a[1], b[1]);
            if (c != 0) return c;
            c = Long.compare(a[2], b[2]);
            if (c != 0) return c;
            return Long.compare(a[3], b[3]);
        });
        Path p = new Path(out.toURI());
        try (SequenceFile.Writer w = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(p),
                SequenceFile.Writer.keyClass(PairKey.class),
                SequenceFile.Writer.valueClass(LocSlotWritable.class),
                SequenceFile.Writer.compression(CompressionType.BLOCK, new DefaultCodec()))) {
            PairKey k = new PairKey();
            LocSlotWritable v = new LocSlotWritable();
            for (long[] wit : witnesses) {
                k.set((int) wit[0], (int) wit[1]);
                v.set((int) wit[2], (int) wit[3]);
                w.append(k, v);
            }
        }
    }

    private static void writeCompanions(List<long[]> rows, File out) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(out.toPath(), StandardCharsets.UTF_8)) {
            for (long[] r : rows) {
                w.write(Long.toString(r[0]));
                w.write(',');
                w.write(Long.toString(r[1]));
                w.write(',');
                w.write(Long.toString(r[2]));
                w.newLine();
            }
        }
    }

    // ===== Helpers =====

    private static File findRepoRoot() throws IOException {
        File cur = new File(".").getCanonicalFile();
        while (cur != null) {
            if (new File(cur, "tests/data/mini.csv").isFile()) return cur;
            cur = cur.getParentFile();
        }
        throw new IllegalStateException(
            "could not locate tests/data/mini.csv by walking up from "
            + new File(".").getCanonicalPath()
            + "; pass repo root as the first argument");
    }

    private static long ms(long startMs) { return System.currentTimeMillis() - startMs; }

    private static void log(String msg) { System.out.println(msg); }
}
