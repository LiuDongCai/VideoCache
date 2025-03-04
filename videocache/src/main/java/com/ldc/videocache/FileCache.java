package com.ldc.videocache;

import android.text.TextUtils;
import android.util.Log;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class FileCache {
    private static final String TAG = "FileCache";
    private final File cacheDir;
    private final String url;
    private File cacheFile;
    private RandomAccessFile randomAccessFile;
    private volatile boolean isClosed;

    public FileCache(String url, File cacheDir) {
        this.url = url;
        this.cacheDir = cacheDir;
        initCache();
    }

    private void initCache() {
        String fileName = generateFileName(url);
        cacheFile = new File(cacheDir, fileName);
        try {
            randomAccessFile = new RandomAccessFile(cacheFile, "rw");
        } catch (IOException e) {
            Log.e(TAG, "Failed to create cache file", e);
        }
    }

    private String generateFileName(String url) {
        String extension = getFileExtension(url);
        String name = md5(url);
        return TextUtils.isEmpty(extension) ? name : name + "." + extension;
    }

    private String getFileExtension(String url) {
        if (url.contains("?")) {
            url = url.substring(0, url.indexOf("?"));
        }
        if (url.contains(".")) {
            String ext = url.substring(url.lastIndexOf(".") + 1);
            if (ext.length() <= 4) {
                return ext;
            }
        }
        return "";
    }

    private String md5(String string) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] bytes = digest.digest(string.getBytes());
            StringBuilder builder = new StringBuilder();
            for (byte b : bytes) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
    }

    public synchronized void write(byte[] data, int length, long position) throws IOException {
        if (isClosed) {
            return;
        }
        randomAccessFile.seek(position);
        randomAccessFile.write(data, 0, length);
    }

    public synchronized byte[] read(long position, int length) throws IOException {
        if (isClosed) {
            return new byte[0];
        }
        byte[] buffer = new byte[length];
        randomAccessFile.seek(position);
        int read = randomAccessFile.read(buffer, 0, length);
        if (read == length) {
            return buffer;
        }
        return Arrays.copyOf(buffer, read);
    }

    public synchronized void close() {
        isClosed = true;
        try {
            if (randomAccessFile != null) {
                randomAccessFile.close();
                randomAccessFile = null;
            }
        } catch (IOException e) {
            Log.e(TAG, "Failed to close cache file", e);
        }
    }

    public File getCacheFile() {
        return cacheFile;
    }

    public String getUrl() {
        return url;
    }

    public boolean exists() {
        return cacheFile != null && cacheFile.exists();
    }

    public long length() {
        return cacheFile != null ? cacheFile.length() : 0;
    }
} 