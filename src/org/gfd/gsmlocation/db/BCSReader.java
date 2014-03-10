package org.gfd.gsmlocation.db;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

/**
 * Reader for compact store files. Compact Store is a key sorted key-value
 * file for primitive types. Key-Value pairs are packed into 4kb blocks. Every
 * byte that does not change is removed from the key and value, thus slightly
 * reducing the payload size.<br />
 * Fileformat:
 * <ol>
 *   <li>4 bytes: Block count - number of 4KB data blocks
 *   <li>blockcount x (keysize * 2 + valuesize * 2 + 4):
 *           min/max keys and values per block + number of entries
 *   <li>4kb blocks, aligend to 4kb boundaries, unchaged blocks are dropped
 * </ol>
 */
public class BCSReader {
    /**
     * Block metadata.
     */
    public final static class BlockMeta {
        public final int blockId;
        public final int count;
        public final byte[][] keyLow;
        public final byte[][] keyHigh;
        public final byte[][] valueLow;
        public final byte[][] valueHigh;
        public BlockMeta(
            int blockId,
            int count,
            byte[][] keyLow,
            byte[][] keyHigh,
            byte[][] valueLow,
            byte[][] valueHigh
        ) {
            this.blockId = blockId;
            this.count = count;
            this.keyLow = keyLow;
            this.keyHigh = keyHigh;
            this.valueLow = valueLow;
            this.valueHigh = valueHigh;
        }
        private String keyString(byte[][] k) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < k.length; i++) {
                byte[] v = k[i];
                if (i == 0) {
                    sb.append("[");
                } else {
                    sb.append(",");
                }
                for (int j = 0; j < v.length; j++) {
                    if (j == 0) {
                        sb.append("[");
                    } else {
                        sb.append(",");
                    }
                    sb.append(v[j] & 0xff);
                }
                sb.append("]");
            }
            sb.append("]");
            return sb.toString();
        }
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("BLOCK(id=");
            sb.append(blockId);
            sb.append(",entries=");
            sb.append(count);
            sb.append(",klow=");
            sb.append(keyString(keyLow));
            sb.append(",khigh=");
            sb.append(keyString(keyHigh));
            sb.append(",vlow=");
            sb.append(keyString(valueLow));
            sb.append(",vhigh=");
            sb.append(keyString(valueHigh));
            return sb.toString();
        }
    }

    /**
     * A single block entry.
     */
    public final static class BlockEntry {
        public Object[] key;
        public Object[] value;
    }

    protected Class<?>[] keyTypes;
    protected Class<?>[] valueTypes;
    protected RandomAccessFile file;
    protected int keySize;
    protected int valueSize;
    protected int blockCount;
    protected int[] keySizes;
    protected int[] valueSizes;

    public BCSReader(
        Class<?>[] keyTypes,
        Class<?>[] valueTypes,
        String file
    ) throws IOException {
        this.keyTypes = keyTypes;
        this.valueTypes = valueTypes;
        this.file = new RandomAccessFile(file, "r");

        // compute the key size
        int keySize = 0;
        int keySizes[] = new int[keyTypes.length];
        for (int i = 0; i < keyTypes.length; i++) {
            Class<?> keyType = keyTypes[i];
            keySizes[i] = type2size(keyType);
            keySize += keySizes[i];
        }
        this.keySize = keySize;
        this.keySizes = keySizes;

        int valueSize = 0;
        int valueSizes[] = new int[valueTypes.length];
        for (int i = 0; i < valueTypes.length; i++) {
            Class<?> valueType = valueTypes[i];
            valueSizes[i] = type2size(valueType);
            valueSize += valueSizes[i];
        }
        this.valueSize = valueSize;
        this.valueSizes = valueSizes;

        // now read the header
        this.file.seek(0l);
        this.blockCount = this.file.readInt();
    }

    /**
     * Convert a java boxed type to the required byte count (e.g. Byte to 1).
     * @param type The boxed java type
     * @return the byte count
     */
    protected int type2size(final Class<?> type) {
        if (type == Byte.class || type == Boolean.class) {
            return 1;
        }
        if (type == Short.class || type == Character.class) {
            return 2;
        }
        if (type == Integer.class || type == Float.class) {
            return 4;
        }
        if (type == Long.class || type == Double.class) {
            return 8;
        }
        return 0;
    }

    protected Object bytes2type(final Class<?> type, byte[] bytes) {
        if (type == Byte.class) {
            return bytes[0];
        }
        if (type == Boolean.class) {
            return bytes[0] == 0;
        }
        if (type == Short.class) {
            return (short)(
                ((bytes[0] & 0xff) << 8) |
                 (bytes[1] & 0xff)
            );
        }
        if (type == Character.class) {
            return (char)(
                ((bytes[0] & 0xff) << 8) |
                 (bytes[1] & 0xff)
            );
        }
        if (type == Integer.class) {
            return (int)(
                ((bytes[0] & 0xff) << 24) |
                ((bytes[1] & 0xff) << 16) |
                ((bytes[2] & 0xff) <<  8) |
                 (bytes[3] & 0xff)
            );
        }
        if (type == Long.class) {
            return (long)(
                ((bytes[0] & 0xffl) << 56) |
                ((bytes[1] & 0xffl) << 48) |
                ((bytes[2] & 0xffl) << 40) |
                ((bytes[3] & 0xffl) << 32) |
                ((bytes[4] & 0xffl) << 24) |
                ((bytes[5] & 0xffl) << 16) |
                ((bytes[6] & 0xffl) <<  8) |
                 (bytes[7] & 0xffl)
            );
        }
        if (type == Float.class) {
            return Float.intBitsToFloat((int)(
                ((bytes[0] & 0xff) << 24) |
                ((bytes[1] & 0xff) << 16) |
                ((bytes[2] & 0xff) <<  8) |
                 (bytes[3] & 0xff)
            ));
        }
        if (type == Double.class) {
            return Double.longBitsToDouble((long)(
                ((bytes[0] & 0xffl) << 56) |
                ((bytes[1] & 0xffl) << 48) |
                ((bytes[2] & 0xffl) << 40) |
                ((bytes[3] & 0xffl) << 32) |
                ((bytes[4] & 0xffl) << 24) |
                ((bytes[5] & 0xffl) << 16) |
                ((bytes[6] & 0xffl) <<  8) |
                 (bytes[7] & 0xffl)
            ));
        }
        return null;
    }

    /**
     * Count the number of bytes needed to store values between the low and
     * high bound.
     * @param low the lower bound.
     * @param high the upper bound.
     * @return The number of bytes.
     */
    protected int requiredSize(byte[][] low, byte[][] high) {
        int size = 0;
        for (int i = 0; i < low.length; i++) {
            byte[] l = low[i];
            byte[] h = high[i];
            boolean required = false;
            for (int j = 0; j < l.length; j++) {
                if (required || l[j] != h[j]) {
                    required = true;
                    size++;
                }
            }
        }
        return size;
    }

    /**
     * Count the number of bytes needed to store each fragment of a 2d byte
     * array.
     * @param low the lower bound.
     * @param high the upper bound.
     * @return The byte counts per fragment.
     */
    protected int[] requiredSizes(byte[][] low, byte[][] high) {
        int[] size = new int[low.length];
        for (int i = 0; i < low.length; i++) {
            byte[] l = low[i];
            byte[] h = high[i];
            boolean required = false;
            int s = 0;
            for (int j = 0; j < l.length; j++) {
                if (required || l[j] != h[j]) {
                    required = true;
                    s++;
                }
            }
            size[i] = s;
        }
        return size;
    }

    /**
     * Convert a boxed java value to a byte array.
     * @param o The boxed java value.
     * @return A byte representation of that value.
     */
    protected byte[] type2bytes(final Object o) {
        // this method, while way to long, does nothing but a big "switch"
        // based on the type.

        if (o instanceof Byte) {
            return new byte[]{
                ((Byte)o).byteValue()
            };
        }
        if (o instanceof Boolean) {
            if (((Boolean)o).booleanValue()) {
                return new byte[]{0};
            } else {
                return new byte[]{1};
            }
        }
        if (o instanceof Short) {
            short s = ((Short)o).shortValue();
            return new byte[] { (byte)(s >> 8), (byte) s };
        }
        if (o instanceof Character) {
            char c = ((Character)o).charValue();
            short s = (short)c;
            return new byte[] {
                (byte)(s >> 8),
                (byte)s
            };
        }
        if (o instanceof Integer) {
            int i = ((Integer)o).intValue();
            return new byte[] {
                (byte)(i >> 24), (byte)(i >> 16),
                (byte)(i >>  8), (byte) i
            };
        }
        if (o instanceof Long) {
            long l = ((Long)o).longValue();
            return new byte[] {
                (byte)(l >> 56), (byte)(l >> 48),
                (byte)(l >> 40), (byte)(l >> 32),
                (byte)(l >> 24), (byte)(l >> 16),
                (byte)(l >>  8), (byte) l
            };
        }
        if (o instanceof Float) {
            int i = Float.floatToIntBits(((Float)o).floatValue());
            return new byte[] {
                (byte)(i >> 24), (byte)(i >> 16),
                (byte)(i >>  8), (byte) i
            };
        }
        if (o instanceof Double) {
            long l = Double.doubleToLongBits(((Double)o).doubleValue());
            return new byte[] {
                (byte)(l >> 56), (byte)(l >> 48),
                (byte)(l >> 40), (byte)(l >> 32),
                (byte)(l >> 24), (byte)(l >> 16),
                (byte)(l >>  8), (byte) l
            };
        }
        if (o instanceof byte[]) {
            return (byte[])o;
        }
        return null;
    }

    /**
     * Offset of block metadata.
     * @param blockid The block id.
     * @return The block metadata offset
     */
    protected int blockMetaOffset(int blockid) {
        /*
         * 4 bytes block count
         * keySize bytes: lower bound
         * keySize bytes: higher bound
         * valueSize bytes: lower bound
         * valueSize bytes: upper bound
         */
        return 4 + (keySize * 2 + valueSize * 2 + 4) * blockid;
    }

    /**
     * Block offset, shifted by header, aligned with 4kb.
     * @param blockid
     * @return
     */
    protected int blockOffset(int blockid) {
        /*
         * Header:
         * 4 bytes block count
         * Per Block: (keySize, keySize, valueSize, valueSize)
         * Header padded to 4kb + blockid * 4kb == offset
         */
        return (
            ((4 + blockCount * (keySize*2 + valueSize*2 + 4)) + 4095) / 4096
        ) * 4096 + blockid * 4096;
    }

    /**
     * Read a data block (4kb).
     * @param blockid The block number.
     * @return The block data.
     * @throws IOException
     */
    protected byte[] readBlock(int blockid) throws IOException {
        byte[] block = new byte[4096];
        int pos = blockOffset(blockid);
        synchronized (file) {
            file.seek(pos);
            file.readFully(block);
        }
        return block;
    }

    /**
     * Read a key at the given offset. Return the byte array.
     * @param pos The file offset, where 0 is the head of the file.
     * @return The key, partitioned by key fragments.
     * @throws IOException
     */
    protected byte[][] readKeyAt(long pos) throws IOException {
        byte buf[] = new byte[keySize];
        synchronized (file) {
            file.seek(pos);
            file.readFully(buf);
        }
        byte res[][] = new byte[keySizes.length][];
        int offset = 0;
        for (int i = 0; i < keySizes.length; i++) {
            byte b[] = new byte[keySizes[i]];
            System.arraycopy(buf, offset, b, 0, b.length);
            offset += b.length;
            res[i] = b;
        }
        return res;
    }

    /**
     * Read a value at the given offset. Return the byte array.
     * @param pos The file offset, where 0 is the head of the file.
     * @return The key, partitioned by key fragments.
     * @throws IOException
     */
    protected byte[][] readValueAt(long pos) throws IOException {
        byte buf[] = new byte[valueSize];
        synchronized (file) {
            file.seek(pos);
            file.readFully(buf);
        }
        byte res[][] = new byte[valueSizes.length][];
        int offset = 0;
        for (int i = 0; i < valueSizes.length; i++) {
            byte b[] = new byte[valueSizes[i]];
            System.arraycopy(buf, offset, b, 0, b.length);
            offset += b.length;
            res[i] = b;
        }
        return res;
    }

    /**
     * Retrieve the number of entries in a 4kb block.
     * @param blockid The block id.
     * @return The number of entries.
     * @throws IOException
     */
    protected int blockEntryCount(int blockid) throws IOException {
        int offset = blockMetaOffset(blockid);
        int i = 0;
        synchronized (file) {
            file.seek(offset);
            i = file.readInt();
        }
        return i;
    }

    /**
     * Retrieve the low block limit for the keys in a block. The value is
     * the same as the first key in the block.
     * @param block the block number.
     * @return The lower bound key value.
     * @throws IOException
     */
    protected byte[][] lowBlockLimit(int block) throws IOException {
        return readKeyAt(blockMetaOffset(block) + 4);
    }

    /**
     * Retrieve the upper bound for the keys in a block. The value is the same
     * as the last key in the block.
     * @param block The block number.
     * @return The upper bound key.
     * @throws IOException
     */
    protected byte[][] highBlockLimit(int block) throws IOException {
        return readKeyAt(blockMetaOffset(block) + 4 + keySize);
    }

    /**
     * Retrieve the minimum value bound for a block.
     * @param block The block number.
     * @return The lower bound.
     * @throws IOException
     */
    protected byte[][] lowBlockValueLimit(int block) throws IOException {
        return readValueAt(blockMetaOffset(block) + 2 * keySize + 4);
    }

    /**
     * Retrieve the maximum value bound for a block.
     * @param block The block number.
     * @return The maximum bound.
     * @throws IOException
     */
    protected byte[][] highBlockValueLimit(int block) throws IOException {
        return readValueAt(blockMetaOffset(block) + 2 * keySize + valueSize + 4);
    }

    /**
     * Retrieve all metadata for a single block.
     * @param blockid The block number.
     * @return The BlockMeta instance.
     * @throws IOException
     */
    protected BlockMeta getBlockMeta(int blockid) throws IOException {
        return new BlockMeta(
            blockid,
            blockEntryCount(blockid),
            lowBlockLimit(blockid),
            highBlockLimit(blockid),
            lowBlockValueLimit(blockid),
            highBlockValueLimit(blockid)
        );
    }

    /**
     * Compare two key arrays up to the maximum level of the shorter one.
     * @param l The left key.
     * @param r The right key.
     * @return -1 if the left key is smaller, 0 if the keys are the same and
     *          1 if the left key is larger than the right key.
     */
    protected int compare(byte[][] l, byte[][] r) {
        final int len = Math.min(l.length, r.length);
        for (int i = 0; i < len; i++) {
            final byte[] li = l[i];
            final byte[] ri = r[i];
            final int leni = Math.min(li.length, ri.length);
            for (int j = 0; j < leni; j++) {
                final int lv = li[j] & 0xff;
                final int rv = ri[j] & 0xff;
                if (lv < rv) {
                    return -1;
                }
                if (lv > rv) {
                    return  1;
                }
            }
        }
        return 0;
    }

    /**
     * Search for the block range containing a key, retrive the BlockMeta
     * information for the first and last block.
     * @param key The key prefix.
     * @param low The current low bound of the search.
     * @param high The current upper bound of the search.
     * @return The block metadata, or null if not found.
     * @throws IOException
     */
    protected BlockMeta[] blockRangeSearch(
        byte[][] key,
        int low,
        int high
    ) throws IOException
    {
        if (low > high) {
            return null;
        }

        final byte[][] lowKey = lowBlockLimit(low);
        final byte[][] highKey = highBlockLimit(high);

        final int cmpLow = compare(lowKey, key);
        if (cmpLow == 1) {
            return null;
        }

        final int cmpHigh = compare(highKey, key);
        if (cmpHigh == -1) {
            return null;
        }

        if (low == high) {
            // we are down to one block, load it and return it as first and
            // last block
            BlockMeta meta = new BlockMeta(
                    low, blockEntryCount(low), lowKey, highKey,
                    lowBlockValueLimit(low), highBlockValueLimit(low));
            return new BlockMeta[]{meta,meta};
        }

        final byte[][] lowHighKey = highBlockLimit(low);
        final byte[][] highLowKey = lowBlockLimit(high);
        final int cmpLowHigh = compare(lowHighKey, key);
        final int cmpHighLow = compare(highLowKey, key);

        if (high - low == 1) {
            // we have to check if we need both, or none, but we handle this
            // before we're searching for a mid.
            if (cmpLowHigh == -1) {
                // we don't need the lower bound
                return blockRangeSearch(key, low + 1, high);
            }
            if (cmpHighLow == 1) {
                // we don't need the lower bound
                return blockRangeSearch(key, low, high - 1);
            }
            return new BlockMeta[]{
                new BlockMeta(
                    low, blockEntryCount(low), lowKey, lowHighKey,
                    lowBlockValueLimit(low), highBlockValueLimit(low)),
                new BlockMeta(
                    high, blockEntryCount(high), highLowKey, highKey,
                    lowBlockValueLimit(high), highBlockValueLimit(high))
            };
        }

        if (cmpLowHigh == 0 && cmpHighLow == 0) {
            // we need the full range
            BlockMeta metaLow = new BlockMeta(
                    low, blockEntryCount(low), lowKey, lowHighKey,
                    lowBlockValueLimit(low), highBlockValueLimit(low));
            BlockMeta metaHigh = new BlockMeta(
                    high, blockEntryCount(high), highLowKey, highKey,
                    lowBlockValueLimit(high), highBlockValueLimit(high));
            return new BlockMeta[]{metaLow, metaHigh};
        }

        int mid = (low + high) / 2;

        final byte lowMidKey[][] = lowBlockLimit(mid);

        final int cmpLowMid = compare(lowMidKey, key);

        if (cmpLowMid == 1) {
            return blockRangeSearch(key, low, mid - 1);
        }

        final byte highMidKey[][] = highBlockLimit(mid);
        final int cmpHighMid = compare(highMidKey, key);

        if (cmpHighMid == -1) {
            return blockRangeSearch(key, mid + 1, high);
        }

        // cmpHighMid == 0 && cmpLowMid == 0
        // This means mid is part of the range

        return new BlockMeta[]{
            blockRangeSearch(key, low, mid)[0],
            blockRangeSearch(key, mid, high)[1]
        };
    }

    /**
     * Search for all occurancies of a given key prefix.
     * @param key The key prefix.
     * @return Array of lower and upper bound metadata.
     * @throws IOException
     */
    protected BlockMeta[] blockRangeSearch(byte[][] key) throws IOException {
        return blockRangeSearch(key, 0, blockCount - 1);
    }

    /**
     * Scan a full block for a single key/value pair identified by key.
     * @param meta The block metadata.
     * @param key The key that should be found.
     * @return The value data or null if not found.
     * @throws IOException
     */
    protected byte[][] scanBlock(BlockMeta meta, byte[][] key) throws IOException {
        byte[] block = readBlock(meta.blockId);
        int offset = 0;

        byte[][] keybuf = new byte[keySizes.length][];
        for (int i = 0; i < keySizes.length; i++) {
            keybuf[i] = new byte[keySizes[i]];
            System.arraycopy(meta.keyLow[i], 0, keybuf[i], 0, keySizes[i]);
        }

        int blockValueSize = requiredSize(meta.valueLow, meta.valueHigh);
        int[] blockKeySizes = requiredSizes(meta.keyLow, meta.keyHigh);
        int[] blockValueSizes = requiredSizes(meta.valueLow, meta.valueHigh);

        int count = meta.count;
        while (count > 0) {
            // load block key
            for (int i = 0; i < keySizes.length; i++) {
                int d = keySizes[i] - blockKeySizes[i];
                if (blockKeySizes[i] > 0) {
                    System.arraycopy(block, offset, keybuf[i], d, blockKeySizes[i]);
                    offset += blockKeySizes[i];
                }
            }
            int cmp = compare(keybuf, key);
            count--;

            if (cmp == -1) {
                offset += blockValueSize;
                continue;
            }
            if (cmp == 0) {
                // we have the right block, decode the value

                // initialize the buffer
                byte[][] valuebuf = new byte[valueSizes.length][];
                for (int i = 0; i < valueSizes.length; i++) {
                    valuebuf[i] = new byte[valueSizes[i]];
                    System.arraycopy(meta.valueLow[i], 0, valuebuf[i], 0, valueSizes[i]);
                }

                // decode
                for (int i = 0; i < valueSizes.length; i++) {
                    int d = valueSizes[i] - blockValueSizes[i];
                    if (blockValueSizes[i] > 0) {
                        System.arraycopy(block, offset, valuebuf[i], d, blockValueSizes[i]);
                        offset += blockValueSizes[i];
                    }
                }

                // return
                return valuebuf;
            }
        }
        return null;
    }

    /**
     * Scan a block for all key/value pairs starting with a given key prefix.
     * @param meta The block metadata.
     * @param key The key prefix.
     * @return Array of block entries or null if not found.
     * @throws IOException
     */
    protected BlockEntry[] scanFullBlock(BlockMeta meta, byte[][] key) throws IOException {
        ArrayList<BlockEntry> entries = new ArrayList<BlockEntry>();

        System.out.println(meta);

        byte[] block = readBlock(meta.blockId);
        int offset = 0;

        byte[][] keybuf = new byte[keySizes.length][];
        for (int i = 0; i < keySizes.length; i++) {
            keybuf[i] = new byte[keySizes[i]];
            System.arraycopy(meta.keyLow[i], 0, keybuf[i], 0, keySizes[i]);
        }

        int blockValueSize = requiredSize(meta.valueLow, meta.valueHigh);
        int[] blockKeySizes = requiredSizes(meta.keyLow, meta.keyHigh);
        int[] blockValueSizes = requiredSizes(meta.valueLow, meta.valueHigh);

        int count = meta.count;
        while (count > 0) {
            for (int i = 0; i < keySizes.length; i++) {
                int d = keySizes[i] - blockKeySizes[i];
                if (blockKeySizes[i] > 0) {
                    System.arraycopy(block, offset, keybuf[i], d, blockKeySizes[i]);
                    offset += blockKeySizes[i];
                }
            }
            count--;
            int cmp = compare(keybuf, key);

            if (cmp < 0) {
                offset += blockValueSize;
                continue;
            }
            if (cmp == 0) {
                // we have a hit, decode value and create a BlockEntry
                byte[][] valuebuf = new byte[valueSizes.length][];
                for (int i = 0; i < valueSizes.length; i++) {
                    valuebuf[i] = new byte[valueSizes[i]];
                    System.arraycopy(meta.valueLow[i], 0, valuebuf[i], 0, valueSizes[i]);
                }

                // decode
                for (int i = 0; i < valueSizes.length; i++) {
                    int d = valueSizes[i] - blockValueSizes[i];
                    if (blockValueSizes[i] > 0) {
                        System.arraycopy(block, offset, valuebuf[i], d, blockValueSizes[i]);
                        offset += blockValueSizes[i];
                    }
                }

                BlockEntry e = new BlockEntry();
                Object[] okey = new Object[keybuf.length];
                for (int i = 0; i < keybuf.length; i++) {
                    okey[i] = bytes2type(keyTypes[i], keybuf[i]);
                }
                e.key = okey;
                Object[] ovalue = new Object[valuebuf.length];
                for (int i = 0; i < valuebuf.length; i++) {
                    ovalue[i] = bytes2type(valueTypes[i], valuebuf[i]);
                }
                e.value = ovalue;
                entries.add(e);
            }
            if (cmp > 0) {
                break;
            }
        }

        return entries.toArray(new BlockEntry[entries.size()]);
    }

    /**
     * Retrieve the value(s) for a given key, or null if the given key can not
     * be found.
     * @param key The key.
     * @return The value(s) associated with the key.
     * @throws IOException
     */
    public Object[] get(Object ... key) throws IOException {
        // encode key
        byte bkey[][] = new byte[key.length][];
        for (int i = 0; i < key.length; i++) {
            bkey[i] = type2bytes(key[i]);
        }
        // we have a multi-byte sequence now, search for the key :-)
        BlockMeta[] meta = blockRangeSearch(bkey);
        if (meta == null) {
            return null;
        }
        // we have a block, scan for value
        byte[][] value = null;
        value = scanBlock(meta[0], bkey);
        if (meta[0].blockId != meta[1].blockId) {
            for (int i = meta[0].blockId + 1; value == null && i <= meta[1].blockId; i++) {
                value = scanBlock(getBlockMeta(i), bkey);
            }
        }
        if (value == null) {
            return null;
        }
        // transform value into
        Object[] result = new Object[value.length];
        for (int i = 0; i < value.length; i++) {
            result[i] = bytes2type(valueTypes[i], value[i]);
        }
        return result;
    }

    /**
     * Retrieve all entries with a given key prefix.
     * @param key The key prefix.
     * @return Array of entries.
     * @throws IOException
     */
    public BlockEntry[] getAll(Object ... key) throws IOException {
        byte bkey[][] = new byte[key.length][];
        for (int i = 0; i < key.length; i++) {
            bkey[i] = type2bytes(key[i]);
        }
        // we have a multi-byte sequence now, search for the key :-)
        BlockMeta[] meta = blockRangeSearch(bkey);
        if (meta == null) {
            return null;
        }
        ArrayList<BlockEntry> entries = new ArrayList<BlockEntry>();
        for (int i = meta[0].blockId; i <= meta[1].blockId; i++) {
            BlockMeta m = getBlockMeta(i);
            BlockEntry[] es = scanFullBlock(m, bkey);
            for (BlockEntry e : es) {
                entries.add(e);
            }
        }
        return entries.toArray(new BlockEntry[entries.size()]);
    }

}
