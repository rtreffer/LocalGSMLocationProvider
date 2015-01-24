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

    protected String TAG = "org.gfd.gsmlocation.GSMService";
    protected CellBasedLocationProvider provider = null;

    @Override
    protected Location update() {
        if (provider == null) {
            Log.d(TAG, "update(): no provider");
            return null;
        }

        CellInfo[] infos = provider.getAll();

        if (infos.length == 0) {
            Log.d(TAG, "update(): no known cell infos");
            return null;
        }

        double lng = 0d, lat = 0d;
        for(CellInfo c : infos) {
            Log.d(TAG, "update(): cell at " + c.lat + "," + c.lng);
            lng += c.lng;
            lat += c.lat;
        }
        lng /= infos.length;
        lat /= infos.length;
        float acc = (float)(800d / infos.length);

        Log.d(TAG, "update(): " + lat + "," + lng + " ±" + acc + "m");
        return LocationHelper.create("gsm", lat, lng, acc);
    }

    @Override
    protected void onOpen() {
        Log.d(TAG, "onOpen()");
        super.onOpen();

        final GSMService service = this;
        provider = new CellBasedLocationProvider() {
            @Override
            public void report() {
                Log.d(TAG, "CellBasedLocationProvider.report()");
                service.report(service.update());
            }
        };

        Handler handler = new Handler(Looper.getMainLooper());
        final Context ctx = getApplicationContext();
        handler.post(new Runnable() {
            public void run() {
                provider.init(ctx);
            }
        });
    }

    @Override
    protected void onClose() {
        Log.d(TAG, "onClose()");
        super.onClose();

        provider.deinit();
        provider = null;
    }

}
