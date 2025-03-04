package com.ldc.videocache;

import android.text.TextUtils;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class M3U8Cache {
    private static final String TAG = "M3U8Cache";
    private static final int MAX_CONSECUTIVE_FAILURES = 3; // Maximum consecutive failure count
    private static final int MAX_TOTAL_FAILURES = 10; // Maximum total failure count
    private static final int MAX_RETRY_COUNT = 3;
    private static final int RETRY_DELAY_MS = 1000;
    private static final int VALIDATION_TIMEOUT = 5000; // 5 seconds for validation
    private static final int MAX_VALIDATION_RETRIES = 2;

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
    private static final float PLAYBACK_START_THRESHOLD = 0.1f; // Play starts when cache reaches 10%
    private boolean hasNotifiedReadyForPlayback = false;

    public interface CacheListener {
        void onProgress(int completed, int total, int failed);
        void onError(String error);
        void onComplete(boolean success, String localPath);
        void onReadyForPlayback(String localPath); // Notify that playback can begin when cache is sufficient
    }

    public M3U8Cache(String url, File cacheDir) {
        this.baseUrl = getBaseUrl(url);
        this.cacheDir = new File(cacheDir, "m3u8");
        this.executor = Executors.newFixedThreadPool(3);
        this.tsUrls = new ArrayList<>();
        this.completedSegments = new AtomicInteger(0);
        this.failedSegments = new AtomicInteger(0);
        this.consecutiveFailures = new AtomicInteger(0);
        if (!this.cacheDir.exists()) {
            this.cacheDir.mkdirs();
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

            // 验证第一个分片的可访问性
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
                // 如果找到了可用的URL，更新baseUrl
                if (!workingUrl.equals(baseUrl + firstSegment)) {
                    String newBaseUrl = workingUrl.substring(0, workingUrl.lastIndexOf('/') + 1);
                    Log.d(TAG, "Updating base URL to: " + newBaseUrl);
                    addFallbackBaseUrl(newBaseUrl);
                }
            }

            for (String segment : segments) {
                if (isCanceled) {
                    Log.d(TAG, "M3U8 caching canceled");
                    break;
                }
                String workingUrl = findWorkingTsUrl(segment);
                if (workingUrl != null) {
                    tsUrls.add(workingUrl);
                    executor.execute(() -> downloadTs(workingUrl));
                } else {
                    Log.e(TAG, "Cannot find working URL for segment: " + segment);
                    failedSegments.incrementAndGet();
                    updateProgress();
                }
            }

            // 等待所有下载任务完成或取消
            executor.shutdown();
            boolean completed = executor.awaitTermination(30, TimeUnit.MINUTES);

            if (completed) {
                Log.d(TAG, String.format("M3U8 caching completed. Total: %d, Completed: %d, Failed: %d",
                        segments.size(), completedSegments.get(), failedSegments.get()));

                if (completedSegments.get() > 0) {
                    saveLocalM3U8(content);
                    if (cacheListener != null) {
                        cacheListener.onComplete(true, new File(cacheDir, "index.m3u8").getAbsolutePath());
                    }
                } else {
                    if (cacheListener != null) {
                        cacheListener.onComplete(false, null);
                    }
                }
            } else {
                Log.e(TAG, "M3U8 caching timed out");
                if (cacheListener != null) {
                    cacheListener.onError("M3U8 caching timed out");
                }
            }

        } catch (IOException e) {
            Log.e(TAG, "Failed to cache m3u8: " + e.getMessage(), e);
            if (cacheListener != null) {
                cacheListener.onError("Failed to cache m3u8: " + e.getMessage());
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "M3U8 caching interrupted", e);
            if (cacheListener != null) {
                cacheListener.onError("M3U8 caching interrupted");
            }
            Thread.currentThread().interrupt();
        } finally {
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

        for (String line : lines) {
            line = line.trim();
            if (!line.startsWith("#") && !line.isEmpty() && line.endsWith(".ts")) {
                segments.add(line);
                Log.d(TAG, "Found segment: " + line);
            }
        }

        if (segments.isEmpty()) {
            Log.w(TAG, "No TS segments found in playlist");
        }

        return segments;
    }

    private void downloadTs(String tsUrl) {
        String fileName = tsUrl.substring(tsUrl.lastIndexOf('/') + 1);
        File tsFile = new File(cacheDir, fileName);
        Log.d(TAG, "Downloading TS file: " + fileName);

        if (tsFile.exists() && tsFile.length() > 0) {
            Log.d(TAG, "TS file already exists: " + fileName);
            completedSegments.incrementAndGet();
            consecutiveFailures.set(0);
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

    private void updateProgress() {
        if (cacheListener != null && !isCanceled) {
            int completed = completedSegments.get();
            int total = tsUrls.size();
            int failed = failedSegments.get();

            cacheListener.onProgress(completed, total, failed);

            // Check if the threshold for starting playback has been reached
            if (!hasNotifiedReadyForPlayback && total > 0) {
                float progress = (float) completed / total;
                if (progress >= PLAYBACK_START_THRESHOLD) {
                    hasNotifiedReadyForPlayback = true;
                    saveLocalM3U8Partial(); // Save partially downloaded M3U8 files
                    if (cacheListener != null) {
                        cacheListener.onReadyForPlayback(new File(cacheDir, "index.m3u8").getAbsolutePath());
                    }
                }
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

        // All files are verified, now write the M3U8 file
        File localM3U8 = new File(cacheDir, "index.m3u8");
        Log.d(TAG, "Saving local M3U8 file: " + localM3U8.getAbsolutePath());

        try (FileWriter writer = new FileWriter(localM3U8)) {
            writer.write("#EXTM3U\n");
            writer.write("#EXT-X-VERSION:3\n");

            // Parse the original content to get the target duration
            float maxDuration = 10.0f;
            String[] lines = content.split("\n");
            for (String line : lines) {
                if (line.startsWith("#EXT-X-TARGETDURATION:")) {
                    try {
                        maxDuration = Float.parseFloat(line.substring(22));
                        break;
                    } catch (NumberFormatException e) {
                        Log.w(TAG, "Failed to parse target duration, using default: " + maxDuration);
                    }
                }
            }

            writer.write("#EXT-X-TARGETDURATION:" + Math.ceil(maxDuration) + "\n");
            writer.write("#EXT-X-MEDIA-SEQUENCE:0\n");
            writer.write("#EXT-X-PLAYLIST-TYPE:VOD\n");

            Map<String, String> extinfMap = new HashMap<>();
            String currentExtinf = "#EXTINF:" + maxDuration + ",";

            // Build EXTINF map from original content
            for (int i = 0; i < lines.length; i++) {
                String line = lines[i].trim();
                if (line.startsWith("#EXTINF:")) {
                    if (i + 1 < lines.length) {
                        String nextLine = lines[i + 1].trim();
                        if (!nextLine.startsWith("#") && nextLine.endsWith(".ts")) {
                            String fileName = nextLine.substring(nextLine.lastIndexOf('/') + 1);
                            extinfMap.put(fileName, line);
                        }
                    }
                }
            }

            // Write segments with their EXTINF tags
            for (String fileName : verifiedSegments) {
                String extinf = extinfMap.getOrDefault(fileName, currentExtinf);
                writer.write(extinf + "\n");
                writer.write(fileName + "\n");
            }

            writer.write("#EXT-X-ENDLIST\n");
        }

        // Verify the written file
        try (BufferedReader reader = new BufferedReader(new FileReader(localM3U8))) {
            String line;
            int segmentCount = 0;
            while ((line = reader.readLine()) != null) {
                if (!line.startsWith("#") && line.endsWith(".ts")) {
                    segmentCount++;
                }
            }
            Log.d(TAG, "Successfully saved local M3U8 file with " + segmentCount + " segments");
            if (segmentCount == 0) {
                Log.e(TAG, "Generated M3U8 file contains no segments!");
                if (cacheListener != null) {
                    cacheListener.onError("Generated M3U8 file contains no segments");
                }
            }
        }
    }

    private void saveLocalM3U8Partial() {
        try {
            File localM3U8 = new File(cacheDir, "index.m3u8");
            Log.d(TAG, "Saving partial M3U8 file: " + localM3U8.getAbsolutePath());

            List<String> verifiedSegments = new ArrayList<>();
            for (String tsUrl : tsUrls) {
                String fileName = tsUrl.substring(tsUrl.lastIndexOf('/') + 1);
                File tsFile = new File(cacheDir, fileName);
                if (tsFile.exists() && tsFile.length() > 0) {
                    verifiedSegments.add(fileName);
                }
            }

            if (verifiedSegments.isEmpty()) {
                Log.w(TAG, "No verified segments available yet");
                return;
            }

            try (FileWriter writer = new FileWriter(localM3U8)) {
                writer.write("#EXTM3U\n");
                writer.write("#EXT-X-VERSION:3\n");
                writer.write("#EXT-X-TARGETDURATION:10\n");
                writer.write("#EXT-X-MEDIA-SEQUENCE:0\n");
                writer.write("#EXT-X-PLAYLIST-TYPE:EVENT\n"); // Use EVENT type to indicate that the playlist may be updated

                // Write downloaded fragments
                for (String fileName : verifiedSegments) {
                    writer.write("#EXTINF:10.0,\n");
                    writer.write(fileName + "\n");
                }

                // Do not write EXT-X-ENDLIST as it is still downloading
            }

            Log.d(TAG, "Successfully saved partial M3U8 file with " + verifiedSegments.size() + " segments");
        } catch (IOException e) {
            Log.e(TAG, "Failed to save partial M3U8 file", e);
        }
    }

    public void updatePartialM3U8() {
        if (isDownloading && hasNotifiedReadyForPlayback) {
            saveLocalM3U8Partial();
        }
    }

    private void verifyTsFiles() {
        boolean allFilesAccessible = true;
        StringBuilder missingFiles = new StringBuilder();

        for (String tsUrl : tsUrls) {
            String fileName = tsUrl.substring(tsUrl.lastIndexOf('/') + 1);
            File tsFile = new File(cacheDir, fileName);
            if (!tsFile.exists() || tsFile.length() == 0) {
                Log.e(TAG, "TS file not accessible: " + fileName);
                allFilesAccessible = false;
                missingFiles.append(fileName).append(", ");
            }
        }

        if (!allFilesAccessible) {
            String error = "The following video clips cannot be accessed: " + missingFiles.toString();
            Log.e(TAG, "Not all TS files are accessible: " + error);
            if (cacheListener != null) {
                cacheListener.onError(error);
            }
        }
    }

    public void cancel() {
        Log.d(TAG, "Canceling M3U8 cache");
        isCanceled = true;
        if (executor != null && !executor.isShutdown()) {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    Log.w(TAG, "Executor did not terminate in the specified time.");
                }
            } catch (InterruptedException e) {
                Log.w(TAG, "Executor shutdown interrupted", e);
                Thread.currentThread().interrupt();
            }
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
}