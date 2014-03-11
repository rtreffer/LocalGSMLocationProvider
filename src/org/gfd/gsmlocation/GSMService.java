package org.gfd.gsmlocation;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.gfd.gsmlocation.model.CellInfo;
import org.microg.nlp.api.LocationBackendService;
import org.microg.nlp.api.LocationHelper;

import android.content.Context;
import android.os.Handler;
import android.os.Looper;
import android.os.Message;
import android.util.Log;

public class GSMService extends LocationBackendService {

    protected String TAG = "o.gfd.gsmlp.LocationBackendService";

    protected Lock lock = new ReentrantLock();
    protected Thread worker = null;

    protected void onOpen() {
        super.onOpen();

        Log.d(TAG, "Starting location backend via looper");

        Looper looper = getApplication().getMainLooper();
        final Context applicationContext = getApplicationContext();

        Handler handler = new Handler(looper) {
            public void handleMessage(Message msg) {
                Log.d(TAG, "Starting location backend in \"UI\" thread");
                CellbasedLocationProvider.getInstance().init(applicationContext);
                try {
                    lock.lock();
                    if (worker != null) worker.interrupt();
                    worker = new Thread() {
                        public void run() {
                            Log.d(TAG, "Starting reporter thread");
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
                                    Log.d(TAG, "report (" + lat + "," + lng + ")");
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
        handler.handleMessage(new Message());
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
