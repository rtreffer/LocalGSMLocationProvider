package org.gfd.gsmlocation;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.gfd.gsmlocation.model.CellInfo;
import org.microg.nlp.api.LocationBackendService;
import org.microg.nlp.api.LocationHelper;

import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;
import android.os.Message;

public class GSMService extends LocationBackendService {

    protected Lock lock = new ReentrantLock();
    protected Thread worker = null;
    protected HandlerThread hthread = null;

    protected void onOpen() {
        super.onOpen();

        hthread = new HandlerThread(
            "GSMNetworkLocationProvider",
            android.os.Process.THREAD_PRIORITY_BACKGROUND);
        hthread.start();
        Looper looper = hthread.getLooper();
        Handler handler = new Handler(looper) {
            public void handleMessage(Message msg) {
                CellbasedLocationProvider.getInstance().init(getApplicationContext());
                try {
                    lock.lock();
                    if (worker != null) worker.interrupt();
                    worker = new Thread() {
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
        };
        handler.handleMessage(null);
    }

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
