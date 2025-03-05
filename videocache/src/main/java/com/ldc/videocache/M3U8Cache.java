package com.ldc.videocache;

import android.text.TextUtils;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.FileOutputStream;

public class M3U8Cache {
    private static final String TAG = "M3U8Cache";
    private static final int MAX_CONSECUTIVE_FAILURES = 3; // Maximum consecutive failure count
    private static final int MAX_TOTAL_FAILURES = 10; // Maximum total failure count
    private static final int MAX_RETRY_COUNT = 3;
    private static final int RETRY_DELAY_MS = 1000;
    private static final int VALIDATION_TIMEOUT = 5000; // 5 seconds for validation
    private static final int BUFFER_SEGMENTS_AHEAD = 8;
    private static final int CORE_POOL_SIZE = 5;
    private static final int MAX_POOL_SIZE = 8;
    private static final int KEEP_ALIVE_TIME = 60; // seconds
    private static final int MINIMUM_SEGMENTS_FOR_PLAYBACK = 3;

    private String baseUrl;
    private final File cacheDir;
    private final ExecutorService executor;
    private final List<String> tsUrls;
    private volatile boolean isCanceled;
    private final AtomicInteger completedSegments;
    private final AtomicInteger failedSegments;
    private final AtomicInteger consecutiveFailures;
    private volatile boolean isDownloading;
    private CacheListener cacheListener;
    private List<String> fallbackBaseUrls = new ArrayList<>();
    private boolean hasNotifiedReadyForPlayback = false;
    private volatile int currentPlayingSegment = 0;
    private Map<String, Float> segmentDurations = new HashMap<>();
    private float defaultSegmentDuration = 10.0f;

    public interface CacheListener {
        void onProgress(int completed, int total, int failed);
        void onError(String error);
        void onComplete(boolean success, String localPath);
        void onReadyForPlayback(String localPath); // Notify that playback can begin when cache is sufficient
    }

    public M3U8Cache(String url, File cacheDir) {
        this.baseUrl = getBaseUrl(url);
        this.cacheDir = new File(cacheDir, "m3u8");
        Log.d(TAG, "Initializing M3U8Cache with cache directory: " + this.cacheDir.getAbsolutePath());
        
        // Create a thread pool that won't shut down automatically
        this.executor = new ThreadPoolExecutor(
            CORE_POOL_SIZE,
            MAX_POOL_SIZE,
            KEEP_ALIVE_TIME,
            TimeUnit.SECONDS,
            new PriorityBlockingQueue<>(),
            new ThreadPoolExecutor.CallerRunsPolicy()
        ) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                if (t != null) {
                    Log.e(TAG, "Task failed with exception", t);
                }
            }
        };
        
        this.tsUrls = new ArrayList<>();
        this.completedSegments = new AtomicInteger(0);
        this.failedSegments = new AtomicInteger(0);
        this.consecutiveFailures = new AtomicInteger(0);
        if (!this.cacheDir.exists()) {
            boolean created = this.cacheDir.mkdirs();
            Log.d(TAG, "Cache directory created: " + created + " at " + this.cacheDir.getAbsolutePath());
        } else {
            Log.d(TAG, "Cache directory already exists at " + this.cacheDir.getAbsolutePath());
        }
    }

    public void setCacheListener(CacheListener listener) {
        this.cacheListener = listener;
    }

    private String getBaseUrl(String url) {
        int lastSlash = url.lastIndexOf('/');
        if (lastSlash > 0) {
            return url.substring(0, lastSlash + 1);
        }
        return url;
    }

    public void addFallbackBaseUrl(String url) {
        if (url != null && !url.isEmpty()) {
            fallbackBaseUrls.add(url);
        }
    }

    private boolean validateTsUrl(String tsUrl) {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(tsUrl).openConnection();
            connection.setRequestMethod("HEAD");
            connection.setConnectTimeout(VALIDATION_TIMEOUT);
            connection.setReadTimeout(VALIDATION_TIMEOUT);
            int responseCode = connection.getResponseCode();
            return responseCode == HttpURLConnection.HTTP_OK;
        } catch (IOException e) {
            Log.w(TAG, "Failed to validate TS URL: " + tsUrl, e);
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private String findWorkingTsUrl(String segment) {
        // First, try the original URL
        String tsUrl = segment.startsWith("http") ? segment : baseUrl + segment;
        if (validateTsUrl(tsUrl)) {
            return tsUrl;
        }

        // Try all alternative URLs
        for (String fallbackBase : fallbackBaseUrls) {
            String fallbackUrl = fallbackBase + segment;
            if (validateTsUrl(fallbackUrl)) {
                Log.d(TAG, "Found working fallback URL: " + fallbackUrl);
                return fallbackUrl;
            }
        }

        return null;
    }

    public void cache(String m3u8Url) {
        isDownloading = true;
        try {
            Log.d(TAG, "Starting to cache M3U8: " + m3u8Url);
            String content = downloadM3U8(m3u8Url);
            if (TextUtils.isEmpty(content)) {
                Log.e(TAG, "Failed to download M3U8 content");
                if (cacheListener != null) {
                    cacheListener.onError("Failed to download M3U8 content");
                }
                return;
            }

            List<String> segments = parseM3U8(content);
            Log.d(TAG, "Found " + segments.size() + " segments in M3U8");

            // Verify the accessibility of the first shard
            if (!segments.isEmpty()) {
                String firstSegment = segments.get(0);
                String workingUrl = findWorkingTsUrl(firstSegment);
                if (workingUrl == null) {
                    String error = "Unable to access video shards, please check if the network connection or video address is valid";
                    Log.e(TAG, error);
                    if (cacheListener != null) {
                        cacheListener.onError(error);
                    }
                    return;
                }
                // If an available URL is found, update the baseURL
                if (!workingUrl.equals(baseUrl + firstSegment)) {
                    String newBaseUrl = workingUrl.substring(0, workingUrl.lastIndexOf('/') + 1);
                    Log.d(TAG, "Updating base URL to: " + newBaseUrl);
                    addFallbackBaseUrl(newBaseUrl);
                }
            }

            // Start downloading segments in background
            Thread downloadThread = new Thread(() -> {
                try {
                    queueSegmentDownloads(segments);
                    
                    // Monitor download progress in background
                    while (isDownloading && !isCanceled) {
                        int completed = completedSegments.get();
                        int failed = failedSegments.get();
                        
                        if (completed + failed == segments.size()) {
                            Log.d(TAG, String.format("M3U8 caching completed. Total: %d, Completed: %d, Failed: %d",
                                    segments.size(), completed, failed));
                            
                            if (completed > 0) {
                                saveLocalM3U8(content);
                                if (cacheListener != null) {
                                    cacheListener.onComplete(true, new File(cacheDir, "index.m3u8").getAbsolutePath());
                                }
                            } else {
                                if (cacheListener != null) {
                                    cacheListener.onComplete(false, null);
                                }
                            }
                            break;
                        }
                        
                        try {
                            Thread.sleep(1000); // Check progress every second
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                } catch (IOException e) {
                    Log.e(TAG, "Error during caching", e);
                    if (cacheListener != null) {
                        cacheListener.onError("Error during caching: " + e.getMessage());
                    }
                } finally {
                    isDownloading = false;
                }
            });
            downloadThread.setName("M3U8CacheMonitor");
            downloadThread.start();

        } catch (IOException e) {
            Log.e(TAG, "Failed to cache m3u8: " + e.getMessage(), e);
            if (cacheListener != null) {
                cacheListener.onError("Failed to cache m3u8: " + e.getMessage());
            }
            isDownloading = false;
        }
    }

    private String downloadM3U8(String url) throws IOException {
        Log.d(TAG, "Downloading M3U8 from: " + url);
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setConnectTimeout(15000);
            connection.setReadTimeout(15000);

            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                Log.e(TAG, "Failed to download M3U8, response code: " + responseCode);
                return null;
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder content = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
            Log.d(TAG, "Successfully downloaded M3U8 content");
            return content.toString();
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private List<String> parseM3U8(String content) {
        List<String> segments = new ArrayList<>();
        String[] lines = content.split("\n");
        boolean isMasterPlaylist = false;
        String selectedPlaylist = null;
        int maxBandwidth = -1;

        // First, check if it is the main playlist
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.startsWith("#EXT-X-STREAM-INF:")) {
                isMasterPlaylist = true;
                // Analyze bandwidth information
                if (line.contains("BANDWIDTH=")) {
                    try {
                        // Obtain bandwidth value
                        String bandwidthStr = line;
                        int bandwidthIndex = bandwidthStr.indexOf("BANDWIDTH=");
                        if (bandwidthIndex != -1) {
                            // Cut from behind BANDWIDTH=
                            bandwidthStr = bandwidthStr.substring(bandwidthIndex + 9);
                            // If there is a comma, only take the part before the comma
                            if (bandwidthStr.contains(",")) {
                                bandwidthStr = bandwidthStr.substring(0, bandwidthStr.indexOf(","));
                            }
                            // Remove possible equal signs
                            bandwidthStr = bandwidthStr.replace("=", "").trim();

                            int bandwidth = Integer.parseInt(bandwidthStr);
                            Log.d(TAG, "Found bandwidth: " + bandwidth);

                            if (bandwidth > maxBandwidth) {
                                maxBandwidth = bandwidth;
                                // Get the next line as the corresponding playlist URL
                                if (i + 1 < lines.length) {
                                    selectedPlaylist = lines[i + 1].trim();
                                    Log.d(TAG, "Selected playlist for bandwidth " + bandwidth + ": " + selectedPlaylist);
                                }
                            }
                        }
                    } catch (NumberFormatException e) {
                        Log.e(TAG, "Failed to parse bandwidth information: " + line, e);
                    }
                }
            }
        }

        if (isMasterPlaylist) {
            Log.d(TAG, "Found master playlist, selecting best quality stream");
            if (selectedPlaylist != null) {
                Log.d(TAG, "Selected playlist: " + selectedPlaylist);
                // Download and parse the selected playlist
                try {
                    String subPlaylistUrl = selectedPlaylist.startsWith("http") ?
                            selectedPlaylist : baseUrl + selectedPlaylist;
                    Log.d(TAG, "Downloading sub-playlist: " + subPlaylistUrl);
                    String subContent = downloadM3U8(subPlaylistUrl);
                    if (!TextUtils.isEmpty(subContent)) {
                        // Update baseURL to the URL of the sub playlist
                        baseUrl = getBaseUrl(subPlaylistUrl);
                        Log.d(TAG, "Updated base URL to: " + baseUrl);
                        return parseM3U8Content(subContent);
                    } else {
                        Log.e(TAG, "Failed to download sub-playlist");
                    }
                } catch (IOException e) {
                    Log.e(TAG, "Error downloading sub-playlist: " + e.getMessage(), e);
                }
            } else {
                Log.e(TAG, "No valid playlist found in master playlist");
            }
            return segments;
        }

        return parseM3U8Content(content);
    }

    private List<String> parseM3U8Content(String content) {
        List<String> segments = new ArrayList<>();
        String[] lines = content.split("\n");
        float currentDuration = -1;
        int segmentCount = 0;

        Log.d(TAG, "Starting to parse M3U8 content...");

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.startsWith("#EXTINF:")) {
                try {
                    String durationStr = line.substring(8);
                    if (durationStr.contains(",")) {
                        durationStr = durationStr.substring(0, durationStr.indexOf(','));
                    }
                    currentDuration = Float.parseFloat(durationStr);
                    Log.d(TAG, "Found segment " + (segmentCount + 1) + " with duration: " + currentDuration);
                } catch (Exception e) {
                    Log.w(TAG, "Failed to parse EXTINF duration: " + line, e);
                    currentDuration = -1;
                }
            } else if (!line.startsWith("#") && !line.isEmpty() && line.endsWith(".ts")) {
                segmentCount++;
                segments.add(line);
                String fileName = line.substring(line.lastIndexOf('/') + 1);
                if (currentDuration > 0) {
                    segmentDurations.put(fileName, currentDuration);
                    Log.d(TAG, String.format("Added segment %d: %s with duration %.3f", segmentCount, fileName, currentDuration));
                } else {
                    Log.w(TAG, "No valid duration found for segment: " + fileName + ", using default duration");
                    segmentDurations.put(fileName, defaultSegmentDuration);
                }
                currentDuration = -1;
            }
        }

        if (segments.isEmpty()) {
            Log.w(TAG, "No TS segments found in playlist");
        } else {
            Log.d(TAG, String.format("Found total of %d segments in M3U8 content", segments.size()));
        }

        return segments;
    }

    private void queueSegmentDownloads(List<String> segments) {
        Log.d(TAG, "Starting to queue segment downloads for " + segments.size() + " segments");
        
        synchronized (tsUrls) {
            tsUrls.clear();
            for (String segment : segments) {
                String fileName = segment.substring(segment.lastIndexOf('/') + 1);
                String fullUrl = segment.startsWith("http") ? segment : baseUrl + segment;
                tsUrls.add(fullUrl);
                Log.d(TAG, "Added segment to tsUrls: " + fileName);
            }
            Log.d(TAG, "Total segments in tsUrls after adding all: " + tsUrls.size());
        }

        // Queue segments with appropriate priorities
        for (int i = 0; i < segments.size(); i++) {
            String segment = segments.get(i);
            String workingUrl = findWorkingTsUrl(segment);
            if (workingUrl != null) {
                int priority;
                if (i < MINIMUM_SEGMENTS_FOR_PLAYBACK) {
                    priority = 1; // Highest priority for minimum required segments
                    Log.d(TAG, "Queueing initial segment with priority 1: " + segment);
                } else if (i < MINIMUM_SEGMENTS_FOR_PLAYBACK + BUFFER_SEGMENTS_AHEAD) {
                    priority = 2; // High priority for buffer segments
                    Log.d(TAG, "Queueing buffer segment with priority 2: " + segment);
                } else {
                    priority = 3; // Normal priority for remaining segments
                    Log.d(TAG, "Queueing remaining segment with priority 3: " + segment);
                }
                
                // Create and queue the download task
                PrioritizedDownloadTask task = new PrioritizedDownloadTask(workingUrl, priority);
                executor.execute(task);
                
                // If this is a high priority segment, wait a bit to ensure order
                if (priority == 1) {
                    try {
                        Thread.sleep(50); // Small delay to help maintain order for critical segments
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } else {
                Log.e(TAG, "Failed to find working URL for segment: " + segment);
            }
        }
        
        Log.d(TAG, "Finished queueing all segment downloads");
    }

    private class PrioritizedDownloadTask implements Runnable, Comparable<PrioritizedDownloadTask> {
        private final String url;
        private final int priority;
        private final int segmentIndex;

        public PrioritizedDownloadTask(String url, int priority) {
            this.url = url;
            this.priority = priority;
            
            // Extract segment index from URL with better error handling
            String fileName = url.substring(url.lastIndexOf('/') + 1);
            int index = -1;
            
            try {
                // First try to find the last underscore
                int underscoreIndex = fileName.lastIndexOf('_');
                if (underscoreIndex >= 0) {
                    String numStr = fileName.substring(underscoreIndex + 1, fileName.lastIndexOf('.'));
                    index = Integer.parseInt(numStr);
                } else {
                    // If no underscore, try to find numbers in the filename
                    String numStr = fileName.replaceAll("[^0-9]", "");
                    if (!numStr.isEmpty()) {
                        index = Integer.parseInt(numStr);
                    }
                }
            } catch (Exception e) {
                Log.w(TAG, "Failed to parse segment index from filename: " + fileName);
            }
            
            this.segmentIndex = index >= 0 ? index : 999999;
            Log.d(TAG, String.format("Created download task for segment %s with index %d and priority %d", 
                fileName, this.segmentIndex, priority));
        }

        @Override
        public void run() {
            if (priority > 1) {
                // Add a smaller delay for background downloads
                try {
                    Thread.sleep(Math.min(25 * segmentIndex, 1000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            downloadTs(url);
        }

        @Override
        public int compareTo(PrioritizedDownloadTask other) {
            // First compare by priority
            int priorityCompare = Integer.compare(this.priority, other.priority);
            if (priorityCompare != 0) {
                return priorityCompare;
            }
            // If same priority, compare by segment index
            return Integer.compare(this.segmentIndex, other.segmentIndex);
        }
    }

    private void downloadTs(String tsUrl) {
        String fileName = tsUrl.substring(tsUrl.lastIndexOf('/') + 1);
        int segmentIndex = getSegmentIndex(fileName);
        
        // Prioritize segments that are coming up soon in playback
        if (segmentIndex > currentPlayingSegment + BUFFER_SEGMENTS_AHEAD) {
            // Delay downloading segments that are far ahead
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        File tsFile = new File(cacheDir, fileName);
        Log.d(TAG, "Downloading TS file: " + fileName);

        if (tsFile.exists() && tsFile.length() > 0) {
            Log.d(TAG, "TS file already exists: " + fileName);
            completedSegments.incrementAndGet();
            consecutiveFailures.set(0);
            // Update M3U8 file to ensure this segment is included
            saveLocalM3U8Partial();
            updateProgress();
            return;
        }

        // Create a temporary file for downloading
        File tempFile = new File(cacheDir, fileName + ".tmp");
        int retryCount = 0;

        while (retryCount < MAX_RETRY_COUNT && !isCanceled) {
            HttpURLConnection connection = null;
            try {
                if (retryCount > 0) {
                    Log.d(TAG, "Retrying download of " + fileName + ", attempt " + (retryCount + 1));
                    Thread.sleep(RETRY_DELAY_MS);

                    String workingUrl = findWorkingTsUrl(fileName);
                    if (workingUrl != null && !workingUrl.equals(tsUrl)) {
                        Log.d(TAG, "Found alternative URL for " + fileName + ": " + workingUrl);
                        tsUrl = workingUrl;
                    }
                }

                connection = (HttpURLConnection) new URL(tsUrl).openConnection();
                connection.setRequestProperty("User-Agent", "Mozilla/5.0");
                connection.setConnectTimeout(15000);
                connection.setReadTimeout(15000);

                int responseCode = connection.getResponseCode();
                if (responseCode != HttpURLConnection.HTTP_OK) {
                    Log.e(TAG, "Failed to download TS file: " + fileName + ", response code: " + responseCode);
                    if (retryCount == MAX_RETRY_COUNT - 1) {
                        failedSegments.incrementAndGet();
                        int consecutive = consecutiveFailures.incrementAndGet();

                        if (consecutive >= MAX_CONSECUTIVE_FAILURES) {
                            String error = "continuous " + MAX_CONSECUTIVE_FAILURES + " downloads failed, stop caching";
                            Log.e(TAG, error);
                            if (cacheListener != null) {
                                cacheListener.onError(error);
                            }
                            cancel();
                            return;
                        }

                        if (failedSegments.get() >= MAX_TOTAL_FAILURES) {
                            String error = "The total number of failures exceeds " + MAX_TOTAL_FAILURES + " times, Stop caching";
                            Log.e(TAG, error);
                            if (cacheListener != null) {
                                cacheListener.onError(error);
                            }
                            cancel();
                            return;
                        }
                    }
                    retryCount++;
                    continue;
                }

                byte[] buffer = new byte[8192];
                int bytesRead;
                long totalBytes = 0;

                try (java.io.InputStream input = connection.getInputStream();
                     java.io.FileOutputStream output = new java.io.FileOutputStream(tempFile)) {

                    while ((bytesRead = input.read(buffer)) != -1) {
                        if (isCanceled) {
                            Log.d(TAG, "TS file download canceled: " + fileName);
                            tempFile.delete();
                            return;
                        }
                        output.write(buffer, 0, bytesRead);
                        totalBytes += bytesRead;
                    }
                    output.flush();
                }

                // Verify the downloaded file
                if (tempFile.exists() && tempFile.length() > 0) {
                    // Rename temp file to final file
                    if (tempFile.renameTo(tsFile)) {
                        Log.d(TAG, String.format("Successfully downloaded TS file: %s, size: %d bytes", fileName, totalBytes));
                        completedSegments.incrementAndGet();
                        consecutiveFailures.set(0);
                        
                        // Important: Update M3U8 file immediately after successful download
                        // This ensures the player can access the new segment right away
                        saveLocalM3U8Partial();
                        
                        updateProgress();
                        return;
                    } else {
                        Log.e(TAG, "Failed to rename temp file to final file: " + fileName);
                    }
                } else {
                    Log.e(TAG, "Downloaded file is empty or does not exist: " + fileName);
                }

            } catch (InterruptedIOException | InterruptedException e) {
                Log.w(TAG, "Download interrupted for TS file: " + fileName + ", attempt " + (retryCount + 1), e);
                if (isCanceled) {
                    tempFile.delete();
                    return;
                }
                retryCount++;
            } catch (IOException e) {
                Log.e(TAG, "Failed to download TS file: " + fileName, e);
                retryCount++;
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }

        // Clean up temp file if download failed
        if (tempFile.exists()) {
            tempFile.delete();
        }

        failedSegments.incrementAndGet();
        updateProgress();
    }

    private int getSegmentIndex(String fileName) {
        try {
            // Extract segment number from filename (assuming format *_XXXXX.ts)
            String numStr = fileName.substring(fileName.lastIndexOf('_') + 1, fileName.lastIndexOf('.'));
            return Integer.parseInt(numStr);
        } catch (Exception e) {
            Log.w(TAG, "Failed to parse segment index from " + fileName);
            return -1;
        }
    }

    private void updateProgress() {
        if (cacheListener != null && !isCanceled) {
            int completed = completedSegments.get();
            int total = tsUrls.size();
            int failed = failedSegments.get();

            cacheListener.onProgress(completed, total, failed);

            // Only notify if this is the first time we're ready for playback
            if (!hasNotifiedReadyForPlayback && total > 0 && completed >= MINIMUM_SEGMENTS_FOR_PLAYBACK) {
                hasNotifiedReadyForPlayback = true;
                cacheListener.onReadyForPlayback(new File(cacheDir, "index.m3u8").getAbsolutePath());
                Log.d(TAG, "Notified ready for playback with " + completed + " segments");
            }
        }
    }

    private void saveLocalM3U8(String content) throws IOException {
        // First verify all TS files
        boolean allFilesAccessible = true;
        StringBuilder missingFiles = new StringBuilder();
        List<String> verifiedSegments = new ArrayList<>();

        for (String tsUrl : tsUrls) {
            String fileName = tsUrl.substring(tsUrl.lastIndexOf('/') + 1);
            File tsFile = new File(cacheDir, fileName);
            if (!tsFile.exists() || tsFile.length() == 0) {
                Log.e(TAG, "TS file not accessible: " + fileName);
                allFilesAccessible = false;
                missingFiles.append(fileName).append(", ");
            } else {
                verifiedSegments.add(fileName);
            }
        }

        if (!allFilesAccessible) {
            String error = "The following video clips cannot be accessed: " + missingFiles.toString();
            Log.e(TAG, "Not all TS files are accessible: " + error);
            if (cacheListener != null) {
                cacheListener.onError(error);
            }
            return;
        }

        if (verifiedSegments.isEmpty()) {
            Log.e(TAG, "No verified segments available to create M3U8 file");
            if (cacheListener != null) {
                cacheListener.onError("No valid segments available");
            }
            return;
        }

        // Sort segments to ensure they are in correct order
        verifiedSegments.sort((a, b) -> {
            try {
                int numA = Integer.parseInt(a.substring(a.lastIndexOf('_') + 1, a.lastIndexOf('.')));
                int numB = Integer.parseInt(b.substring(b.lastIndexOf('_') + 1, b.lastIndexOf('.')));
                return Integer.compare(numA, numB);
            } catch (Exception e) {
                return a.compareTo(b);
            }
        });

        // All files are verified, now write the M3U8 file
        File localM3U8 = new File(cacheDir, "index.m3u8");
        Log.d(TAG, "Saving local M3U8 file: " + localM3U8.getAbsolutePath());

        try (FileWriter writer = new FileWriter(localM3U8)) {
            writer.write("#EXTM3U\n");
            writer.write("#EXT-X-VERSION:3\n");

            // Find maximum segment duration
            float maxDuration = 0;
            for (String fileName : verifiedSegments) {
                maxDuration = Math.max(maxDuration, getSegmentDuration(fileName));
            }
            writer.write("#EXT-X-TARGETDURATION:" + Math.ceil(maxDuration) + "\n");

            writer.write("#EXT-X-MEDIA-SEQUENCE:0\n");
            writer.write("#EXT-X-PLAYLIST-TYPE:VOD\n");

            // Write segments with their actual durations
            float totalDuration = 0;
            for (String fileName : verifiedSegments) {
                float duration = getSegmentDuration(fileName);
                totalDuration += duration;
                writer.write(String.format("#EXTINF:%.3f,\n", duration));
                writer.write(fileName + "\n");
            }

            writer.write("#EXT-X-ENDLIST\n");
            Log.d(TAG, String.format("Successfully saved local M3U8 file with %d segments, total duration: %.3f seconds", 
                verifiedSegments.size(), totalDuration));
        }
    }

    private void saveLocalM3U8Partial() {
        try {
            File localM3U8 = new File(cacheDir, "index.m3u8");
            Log.d(TAG, "Saving partial M3U8 file: " + localM3U8.getAbsolutePath());

            // Create empty.ts if it doesn't exist
            File emptyTs = new File(cacheDir, "empty.ts");
            if (!emptyTs.exists() || emptyTs.length() == 0) {
                try (FileOutputStream fos = new FileOutputStream(emptyTs)) {
                    // Write a valid TS packet
                    byte[] tsPacket = new byte[188];
                    tsPacket[0] = 0x47; // Sync byte
                    tsPacket[1] = 0x1F; // PID 0x1FFF (null packet)
                    tsPacket[2] = (byte) 0xFF;
                    tsPacket[3] = 0x10; // Payload only

                    // Write multiple packets
                    for (int i = 0; i < 1000; i++) {
                        fos.write(tsPacket);
                    }
                }
                Log.d(TAG, "Created empty.ts file with valid TS packets");
            }

            // Collect and sort segments
            List<String> orderedSegments = new ArrayList<>();
            Map<String, Boolean> segmentDownloaded = new HashMap<>();
            float maxDuration = 0;
            
            synchronized (tsUrls) {
                for (String tsUrl : tsUrls) {
                    String fileName = tsUrl.substring(tsUrl.lastIndexOf('/') + 1);
                    float duration = getSegmentDuration(fileName);
                    maxDuration = Math.max(maxDuration, duration);
                    
                    File tsFile = new File(cacheDir, fileName);
                    segmentDownloaded.put(fileName, tsFile.exists() && tsFile.length() > 0);
                    orderedSegments.add(fileName);
                }
            }
            
            // Sort segments by index
            orderedSegments.sort((a, b) -> {
                try {
                    int numA = Integer.parseInt(a.substring(a.lastIndexOf('_') + 1, a.lastIndexOf('.')));
                    int numB = Integer.parseInt(b.substring(b.lastIndexOf('_') + 1, b.lastIndexOf('.')));
                    return Integer.compare(numA, numB);
                } catch (Exception e) {
                    return a.compareTo(b);
                }
            });

            // Write M3U8 content
            StringBuilder m3u8Content = new StringBuilder();
            m3u8Content.append("#EXTM3U\n");
            m3u8Content.append("#EXT-X-VERSION:3\n");
            m3u8Content.append("#EXT-X-TARGETDURATION:").append(Math.ceil(maxDuration)).append("\n");
            m3u8Content.append("#EXT-X-MEDIA-SEQUENCE:0\n");
            m3u8Content.append("#EXT-X-PLAYLIST-TYPE:VOD\n");
            m3u8Content.append("#EXT-X-START:TIME-OFFSET=0\n");

            // Write all segments to maintain total duration
            float totalDuration = 0;
            int downloadedCount = 0;
            
            for (String fileName : orderedSegments) {
                float duration = getSegmentDuration(fileName);
                totalDuration += duration;
                m3u8Content.append(String.format("#EXTINF:%.3f,\n", duration));
                
                if (segmentDownloaded.get(fileName)) {
                    m3u8Content.append(fileName).append("\n");
                    downloadedCount++;
                } else {
                    // For undownloaded segments, use the actual segment name instead of empty.ts
                    m3u8Content.append(fileName).append("\n");
                }
            }

            // Always add ENDLIST tag to ensure total duration is shown
            m3u8Content.append("#EXT-X-ENDLIST\n");

            // Write the content to file
            try (FileWriter writer = new FileWriter(localM3U8)) {
                writer.write(m3u8Content.toString());
            }

            Log.d(TAG, String.format("M3U8 update complete - Downloaded: %d/%d segments, Total duration: %.3f seconds", 
                downloadedCount, orderedSegments.size(), totalDuration));

            // Notify ready for playback if we have enough segments
            if (cacheListener != null && !hasNotifiedReadyForPlayback && downloadedCount >= MINIMUM_SEGMENTS_FOR_PLAYBACK) {
                hasNotifiedReadyForPlayback = true;
                cacheListener.onReadyForPlayback(localM3U8.getAbsolutePath());
                Log.d(TAG, "Notified ready for playback with " + downloadedCount + " segments");
            }

        } catch (IOException e) {
            Log.e(TAG, "Failed to save partial M3U8 file", e);
            e.printStackTrace();
        }
    }

    public void updatePartialM3U8() {
        if (isDownloading && hasNotifiedReadyForPlayback) {
            saveLocalM3U8Partial();
        }
    }

    public void cancel() {
        Log.d(TAG, "Canceling M3U8 cache");
        isCanceled = true;
        if (executor != null) {
            List<Runnable> pendingTasks = executor.shutdownNow();
            Log.d(TAG, "Canceled " + pendingTasks.size() + " pending download tasks");
        }
    }

    public boolean isCompleted() {
        if (isDownloading) {
            return false;
        }

        if (tsUrls.isEmpty()) {
            return false;
        }

        int completed = completedSegments.get();
        int total = tsUrls.size();
        Log.d(TAG, String.format("Cache status: %d/%d segments completed", completed, total));

        return completed == total;
    }

    public String getLocalM3U8Path() {
        File localM3U8 = new File(cacheDir, "index.m3u8");
        return localM3U8.exists() ? localM3U8.getAbsolutePath() : null;
    }

    public void setCurrentPlayingSegment(int segmentIndex) {
        this.currentPlayingSegment = segmentIndex;
    }

    public float getSegmentDuration(String fileName) {
        return segmentDurations.getOrDefault(fileName, defaultSegmentDuration);
    }

    public float getTotalDurationUpToSegment(int segmentIndex) {
        float totalDuration = 0;
        int currentIndex = 0;
        for (String tsUrl : tsUrls) {
            if (currentIndex >= segmentIndex) break;
            String fileName = tsUrl.substring(tsUrl.lastIndexOf('/') + 1);
            totalDuration += getSegmentDuration(fileName);
            currentIndex++;
        }
        return totalDuration;
    }

    public int getTotalSegments() {
        synchronized (tsUrls) {
            return tsUrls.size();
        }
    }

    public String getSegmentFileName(int index) {
        synchronized (tsUrls) {
            if (index < 0 || index >= tsUrls.size()) {
                return null;
            }
            String tsUrl = tsUrls.get(index);
            return tsUrl.substring(tsUrl.lastIndexOf('/') + 1);
        }
    }

    public void ensureSegmentsCached(int startSegment, int endSegment) {
        List<String> urlsToCache = new ArrayList<>();
        synchronized (tsUrls) {
            if (startSegment < 0 || endSegment >= tsUrls.size() || startSegment > endSegment) {
                Log.w(TAG, "Invalid segment range: " + startSegment + " to " + endSegment);
                return;
            }

            // Always try to cache BUFFER_SEGMENTS_AHEAD segments ahead of the current segment
            endSegment = Math.min(startSegment + BUFFER_SEGMENTS_AHEAD, tsUrls.size() - 1);

            for (int i = startSegment; i <= endSegment; i++) {
                String tsUrl = tsUrls.get(i);
                String fileName = tsUrl.substring(tsUrl.lastIndexOf('/') + 1);
                File tsFile = new File(cacheDir, fileName);
                
                if (!tsFile.exists() || tsFile.length() == 0) {
                    urlsToCache.add(tsUrl);
                }
            }
        }

        // Queue downloads outside of synchronized block with high priority
        for (String tsUrl : urlsToCache) {
            executor.execute(new PrioritizedDownloadTask(tsUrl, 1)); // Priority 1 for immediate segments
        }

        // Also queue next batch of segments with lower priority
        int nextStartSegment = endSegment + 1;
        int nextEndSegment = Math.min(nextStartSegment + BUFFER_SEGMENTS_AHEAD, getTotalSegments() - 1);
        
        if (nextStartSegment < getTotalSegments()) {
            List<String> nextUrlsToCache = new ArrayList<>();
            synchronized (tsUrls) {
                for (int i = nextStartSegment; i <= nextEndSegment; i++) {
                    String tsUrl = tsUrls.get(i);
                    String fileName = tsUrl.substring(tsUrl.lastIndexOf('/') + 1);
                    File tsFile = new File(cacheDir, fileName);
                    
                    if (!tsFile.exists() || tsFile.length() == 0) {
                        nextUrlsToCache.add(tsUrl);
                    }
                }
            }
            
            // Queue next batch with lower priority
            for (String tsUrl : nextUrlsToCache) {
                executor.execute(new PrioritizedDownloadTask(tsUrl, 2)); // Priority 2 for future segments
            }
        }
    }
}