package org.gfd.gsmlocation;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.gfd.gsmlocation.model.CellInfo;
import org.microg.nlp.api.LocationBackendService;
import org.microg.nlp.api.LocationHelper;

import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.location.Location;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.util.Log;

public class GSMService extends LocationBackendService {

    protected String TAG = "o.gfd.gsmlp.LocationBackendService";

    protected Lock lock = new ReentrantLock();
    protected Thread worker = null;
    protected CellbasedLocationProvider lp = null;

    public void start() {
        if (worker != null && worker.isAlive()) return;

        Log.d(TAG, "Starting location backend");
        Handler handler = new Handler(Looper.getMainLooper());
        final Context ctx = getApplicationContext();
        handler.post(new Runnable() {
            public void run() {
                CellbasedLocationProvider.getInstance().init(ctx);
            }
        });
        try {
            lock.lock();
            if (worker != null && worker.isAlive()) worker.interrupt();
            worker = new Thread() {
                public void run() {
                    Log.d(TAG, "Starting reporter thread");
                    lp = CellbasedLocationProvider.getInstance();
                    double lastLng = 0d;
                    double lastLat = 0d;
                    try { while (true) {
                        Thread.sleep(1000);

                        try {
                        CellInfo[] infos = lp.getAll();

                        if (infos.length == 0) continue;

                        double lng = 0d;
                        double lat = 0d;
                        for(CellInfo c : infos) {
                            lng += c.lng;
                            lat += c.lat;
                        }
                        lng /= infos.length;
                        lat /= infos.length;
                        float acc = (float)(800d / infos.length);
                        if (lng != lastLng || lat != lastLat) {
                            Log.d(TAG, "report (" + lat + "," + lng + ")");
                            lastLng = lng;
                            lastLat = lat;
                            report(LocationHelper.create("gsm", lat, lng, acc));
                        }
                        } catch (Exception e) {
                            Log.e(TAG, "Update loop failed", e);
                        }
                    } } catch (InterruptedException e) {}
                }
            };
            worker.start();
        } catch (Exception e) {
            Log.e(TAG, "Start failed", e);
        } finally {
            try { lock.unlock(); } catch (Exception e) {}
        }
    }

    @Override
    protected Location update() {
        start();

        CellInfo[] infos = lp.getAll();

        if (infos.length == 0) return null;

        double lng = 0d;
        double lat = 0d;
        for(CellInfo c : infos) {
            lng += c.lng;
            lat += c.lat;
        }
        lng /= infos.length;
        lat /= infos.length;
        float acc = (float)(800d / infos.length);

        Log.d(TAG, "update (" + lat + "," + lng + ")");
        return LocationHelper.create("gsm", lat, lng, acc);
    }

    @Override
    protected void onOpen() {
        super.onOpen();

        start();

        Log.d(TAG, "Binder OPEN called");
    }

    protected void onClose() {
        Log.d(TAG, "Binder CLOSE called");
        super.onClose();
        try {
            lock.lock();
            if (worker != null && worker.isAlive()) worker.interrupt();
            if (worker != null) worker = null;
        } finally {
            try { lock.unlock(); } catch (Exception e) {}
        }
    }

}
