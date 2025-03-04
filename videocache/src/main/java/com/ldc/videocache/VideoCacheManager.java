package com.ldc.videocache;

import android.content.Context;
import android.net.Uri;
import android.text.TextUtils;
import android.util.Log;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentHashMap;

public class VideoCacheManager {
    private static final String TAG = "VideoCacheManager";
    private static final int DEFAULT_PORT = 8080;
    private static VideoCacheManager instance;
    private final Context context;
    private ProxyServer proxyServer;
    private final ConcurrentHashMap<String, FileCache> cacheMap;
    private int port;
    private final ConcurrentHashMap<String, CacheListener> listeners;

    public interface CacheListener {
        void onCacheProgress(String url, int percentsAvailable);
        void onCacheAvailable(String url, File file);
        void onCacheError(String url, int percentsAvailable, Exception e);
    }

    private VideoCacheManager(Context context) {
        this.context = context.getApplicationContext();
        this.cacheMap = new ConcurrentHashMap<>();
        this.listeners = new ConcurrentHashMap<>();
        init();
    }

    public static VideoCacheManager getInstance(Context context) {
        if (instance == null) {
            synchronized (VideoCacheManager.class) {
                if (instance == null) {
                    instance = new VideoCacheManager(context);
                }
            }
        }
        return instance;
    }

    private void init() {
        port = getAvailablePort();
        startProxy();
    }

    private void startProxy() {
        proxyServer = new ProxyServer(port, context);
        proxyServer.setCacheListener(new ProxyServer.CacheListener() {
            @Override
            public void onCacheProgress(String url, int percentsAvailable) {
                CacheListener listener = listeners.get(url);
                if (listener != null) {
                    listener.onCacheProgress(url, percentsAvailable);
                }
            }

            @Override
            public void onCacheAvailable(String url, File file) {
                CacheListener listener = listeners.get(url);
                if (listener != null) {
                    listener.onCacheAvailable(url, file);
                }
            }

            @Override
            public void onCacheError(String url, int percentsAvailable, Exception e) {
                CacheListener listener = listeners.get(url);
                if (listener != null) {
                    listener.onCacheError(url, percentsAvailable, e);
                }
            }
        });

        new Thread(() -> {
            try {
                proxyServer.start();
            } catch (IOException e) {
                Log.e(TAG, "Failed to start proxy server", e);
            }
        }).start();

        // Waiting for the proxy server to start
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Log.e(TAG, "Sleep interrupted", e);
        }
    }

    private int getAvailablePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(DEFAULT_PORT);
            return DEFAULT_PORT;
        } catch (IOException e) {
            try {
                if (socket != null) {
                    socket.close();
                }
                socket = new ServerSocket(0);
                int port = socket.getLocalPort();
                socket.close();
                return port;
            } catch (IOException e1) {
                return DEFAULT_PORT;
            }
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    Log.e(TAG, "Error closing socket", e);
                }
            }
        }
    }

    public String getProxyUrl(String url) {
        if (TextUtils.isEmpty(url)) {
            return url;
        }

        // Remove the protocol section from the URL
        String processedUrl = url;
        if (processedUrl.startsWith("http://")) {
            processedUrl = processedUrl.substring("http://".length());
        } else if (processedUrl.startsWith("https://")) {
            processedUrl = processedUrl.substring("https://".length());
        }

        // Encoding URL
        String encodedUrl = Uri.encode(processedUrl);
        String proxyUrl = "http://127.0.0.1:" + port + "/" + encodedUrl;
        Log.d(TAG, "Original URL: " + url);
        Log.d(TAG, "Processed URL: " + processedUrl);
        Log.d(TAG, "Proxy URL: " + proxyUrl);
        return proxyUrl;
    }

    public void registerCacheListener(CacheListener listener, String url) {
        if (listener != null && url != null) {
            listeners.put(url, listener);
        }
    }

    public void unregisterCacheListener(String url) {
        if (url != null) {
            listeners.remove(url);
        }
    }

    public void unregisterAllCacheListeners() {
        listeners.clear();
    }

    public void release() {
        unregisterAllCacheListeners();
        if (proxyServer != null) {
            proxyServer.stop();
            proxyServer = null;
        }
        cacheMap.clear();
        instance = null;
    }

    public File getCacheDir() {
        File cacheDir = new File(context.getCacheDir(), "video-cache");
        if (!cacheDir.exists()) {
            cacheDir.mkdirs();
        }
        return cacheDir;
    }

    FileCache getFileCache(String url) {
        FileCache cache = cacheMap.get(url);
        if (cache == null) {
            cache = new FileCache(url, getCacheDir());
            cacheMap.put(url, cache);
        }
        return cache;
    }
}