package org.gfd.gsmlocation;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.location.Location;
import android.util.Log;

import org.gfd.gsmlocation.model.CellInfo;
import org.microg.nlp.api.LocationBackendService;
import org.microg.nlp.api.LocationHelper;

public class GSMService extends LocationBackendService {

    private Lock lock = new ReentrantLock();
    private Thread worker = null;

    @Override
    protected void onOpen() {
        super.onOpen();
        CellbasedLocationProvider.getInstance().init(getApplicationContext());
        try {
            lock.lock();
            if (worker != null) worker.interrupt();
            worker = new Thread() {
                @Override
                public void run() {
                    CellbasedLocationProvider lp =
                        CellbasedLocationProvider.getInstance();
                    double lastLng = 0d;
                    double lastLat = 0d;
                    try { while (true) {
                        Thread.sleep(1000);

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
                            lastLng = lng;
                            lastLat = lat;
                            report(LocationHelper.create("gsm", lat, lng, acc));
                        }
                    } } catch (InterruptedException e) {}
                }
            };
            worker.start();
        } finally {
            try { lock.unlock(); } catch (Exception e) {}
        }
    }

    @Override
    protected void onClose() {
        super.onClose();
        try {
            lock.lock();
            if (worker != null) worker.interrupt();
        } finally {
            try { lock.unlock(); } catch (Exception e) {}
        }
    }
}
