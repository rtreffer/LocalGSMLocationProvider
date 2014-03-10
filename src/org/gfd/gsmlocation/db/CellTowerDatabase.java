package org.gfd.gsmlocation.db;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.gfd.gsmlocation.R;
import org.gfd.gsmlocation.model.CellInfo;
import org.tukaani.xz.XZInputStream;

import android.content.Context;
import android.telephony.NeighboringCellInfo;
import android.util.Log;
import android.util.LruCache;

public class CellTowerDatabase {

    private static CellTowerDatabase ourInstance = new CellTowerDatabase();

    public static CellTowerDatabase getInstance() {
        return ourInstance;
    }

    private BCSReader reader = null;

    private CellTowerDatabase() {}

    /**
     * Initialize the DB, possibly copying the content over to another file. Note that this method
     * may require considerable amounts of time.
     * @param ctx The app context.
     */
    public void init(Context ctx) {
        final int dbfilesize = ctx.getResources().getInteger(R.integer.dbfile_size);
        final String dbfilename = ctx.getResources().getString(R.string.dbfile);

        File path = ctx.getDatabasePath("towers");
        path.mkdirs();
        File db = new File(path + "/db.bcs");
        android.util.Log.d("SS/CellTowerDatabase/Init", "Path: " + path);
        if (!db.exists() || db.length() < dbfilesize) {
            android.util.Log.d("SS/CellTowerDatabase/Init", "Database needs extraction...");
            // extract. This can take *quite* some time.
            try {
                InputStream in = ctx.getAssets().open(dbfilename);
                OutputStream out = new BufferedOutputStream(new FileOutputStream(db));
                XZInputStream xz = new XZInputStream(in);
                byte[] buf = new byte[16 * 1024];
                boolean canread = true;
                while (canread) {
                    final int read = xz.read(buf);
                    if (read > 0) {
                        out.write(buf, 0, read);
                    } else {
                        final int b = xz.read();
                        if (b == -1) {
                            canread = false;
                        } else {
                            out.write(b);
                        }
                    }
                }
                out.close();
                xz.close();
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            android.util.Log.d("SS/CellTowerDatabase/Init", "Database extracted!");
        }
        android.util.Log.d("SS/CellTowerDatabase/Init", "Opening database");
        try {
            reader = new BCSReader(
                new Class<?>[]{Integer.class, Integer.class, Integer.class, Integer.class},
                new Class<?>[]{Double.class, Double.class},
                db.getCanonicalPath()
            );
        } catch (IOException e) {
            Log.e("LNLP", "init failed", e);
        }
    }

    /**
     * Used internally for caching. HashMap compatible entity class.
     */
    private static class QueryArgs {
        Integer mcc;
        Integer mnc;
        int cid;
        int lac;

        private QueryArgs(Integer mcc, Integer mnc, int cid, int lac) {
            this.mcc = mcc;
            this.mnc = mnc;
            this.cid = cid;
            this.lac = lac;
        }
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            QueryArgs queryArgs = (QueryArgs) o;

            if (cid != queryArgs.cid) return false;
            if (lac != queryArgs.lac) return false;
            if (mcc != null ? !mcc.equals(queryArgs.mcc) : queryArgs.mcc != null) return false;
            if (mnc != null ? !mnc.equals(queryArgs.mnc) : queryArgs.mnc != null) return false;

            return true;
        }
        public int hashCode() {
            int result = mcc != null ? mcc.hashCode() : (1 << 16);
            result = 31 * result + (mnc != null ? mnc.hashCode() : (1 << 16));
            result = 31 * result + cid;
            result = 31 * result + lac;
            return result;
        }

    }

    /**
     * DB negative query cache (not found in db).
     */
    private final LruCache<QueryArgs, Boolean> queryResultNegativeCache =
            new LruCache<QueryArgs, Boolean>(10000);
    /**
     * DB positive query cache (found in the db).
     */
    private final LruCache<QueryArgs, List<CellInfo>> queryResultCache =
            new LruCache<QueryArgs, List<CellInfo>>(10000);

    public List<CellInfo> query(final int cid, final int lac) {
        return query(null, null, cid, lac);
    }

    /**
     * Perform a (cached) DB query for a given cell tower. Note that MCC and MNC can be null.
     * @param mcc
     * @param mnc
     * @param cid
     * @param lac
     * @return
     */
    public List<CellInfo> query(final Integer mcc, final Integer mnc, final int cid, final int lac) {
        if (this.reader == null) return null;

        if (cid == NeighboringCellInfo.UNKNOWN_CID || cid == Integer.MAX_VALUE) return null;

        if (mcc != null && mcc == Integer.MAX_VALUE) return query(null, mnc, cid, lac);
        if (mnc != null && mnc == Integer.MAX_VALUE) return query(mcc, null, cid, lac);

        QueryArgs args = new QueryArgs(mcc, mnc, cid, lac);
        Boolean negative = queryResultNegativeCache.get(args);
        if (negative != null && negative.booleanValue()) return null;

        List<CellInfo> cached = queryResultCache.get(args);
        if (cached != null) return cached;

        List<CellInfo> result = _query(mcc, mnc, cid, lac);

        if (result == null) {
            queryResultNegativeCache.put(args, true);
            return null;
        }

        result = Collections.unmodifiableList(result);

        queryResultCache.put(args, result);
        return result;
    }

    /**
     * Internal db query to retrieve all cell tower candidates for a given cid/lac.
     * @param mcc
     * @param mnc
     * @param cid
     * @param lac
     * @return
     */
    private List<CellInfo> _query(Integer mcc, Integer mnc, int cid, int lac) {
        if (this.reader == null) return null;

        // we need at least CID/LAC
        if (cid == NeighboringCellInfo.UNKNOWN_CID) return null;

        android.util.Log.d("LNLP/Query", "(" + mcc + "," + mnc + "," + cid + "," + lac + ")");

        List<CellInfo> cil = _queryDirect(mcc, mnc, cid, lac);
        if (cil == null || cil.size() == 0) {
            if (cid > 0xffff) {
                _queryDirect(mcc, mnc, cid & 0xffff, lac);
            }
        }
        if (cil != null && cil.size() > 0) {
            return cil;
        }

        if (mcc != null && mnc != null) {
            return query(mcc, null, cid, lac);
        }

        if (mcc != null || mnc != null) {
            return query(null,null,cid,lac);
        }

        return null;
    }

    private List<CellInfo> _queryDirect(Integer mcc, Integer mnc, int cid, int lac) {
        if (mcc != null && mnc != null) {
            // try direct lookup
            Object[] values;
            try {
                values = reader.get(lac, cid, mcc, mnc);
            } catch (IOException e) {
                Log.e("LNLP", "queryDirect failed", e);
                return null; // We're broken
            }
            if (values != null) {
                CellInfo ci = new CellInfo();
                ci.CID = cid;
                ci.LAC = lac;
                ci.MCC = mcc;
                ci.MNC = mnc;
                ci.lng = (Double) values[0];
                ci.lat = (Double) values[1];
                return Arrays.asList(new CellInfo[]{ci});
            }
            return null;
        }
        if (mnc != null && mcc == null) {
            // this is special, we can only search for cid + lac and must filter afterwards
            BCSReader.BlockEntry[] be;
            try {
                be = reader.getAll(lac, cid);
            } catch (IOException e) {
                Log.e("LNLP", "queryDirect failed", e);
                return null; // br0ke
            }
            if (be != null && be.length > 0) {
                ArrayList<CellInfo> cil = new ArrayList<CellInfo>();
                for (BCSReader.BlockEntry e : be) {
                    if (((Integer) e.key[3]).equals(mnc)) {
                        CellInfo ci = new CellInfo();
                        ci.CID = (Integer) e.key[1];
                        ci.LAC = (Integer) e.key[0];
                        ci.MCC = (Integer) e.key[2];
                        ci.MNC = (Integer) e.key[3];
                        ci.lng = (Double) e.value[0];
                        ci.lat = (Double) e.value[1];
                        cil.add(ci);
                    }
                }
                if (!cil.isEmpty()) {
                    return cil;
                }
            }
            return null;
        }
        BCSReader.BlockEntry[] be;
        if (mcc != null) {
            try {
                be = reader.getAll(lac, cid, mcc);
            } catch (IOException e) {
                Log.e("LNLP", "queryDirect failed", e);
                return null; // br0ke
            }
        } else {
            try {
                be = reader.getAll(lac, cid);
            } catch (IOException e) {
                Log.e("LNLP", "queryDirect failed", e);
                return null; // br0ke
            }
        }
        if (be == null || be.length == 0) {
            return null;
        }
        ArrayList<CellInfo> cil = new ArrayList<CellInfo>();
        for (BCSReader.BlockEntry e : be) {
            CellInfo ci = new CellInfo();
            ci.CID = (Integer) e.key[1];
            ci.LAC = (Integer) e.key[0];
            ci.MCC = (Integer) e.key[2];
            ci.MNC = (Integer) e.key[3];
            ci.lng = (Double) e.value[0];
            ci.lat = (Double) e.value[1];
            cil.add(ci);
        }
        return cil;
    }

}
