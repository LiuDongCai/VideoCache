package com.ldc.videocache;

import android.content.Context;
import android.net.Uri;
import android.text.TextUtils;
import android.util.Log;
import android.webkit.MimeTypeMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class ProxyServer {
    private static final String TAG = "ProxyServer";
    private static final int BUFFER_SIZE = 8192;
    private static final String MIME_TYPE_MP4 = "video/mp4";
    private static final String MIME_TYPE_WEBM = "video/webm";
    private static final String[] TLS_VERSIONS = {"TLSv1.3", "TLSv1.2", "TLSv1.1", "TLSv1"};
    private final int port;
    private final Context context;
    private ServerSocket serverSocket;
    private final ExecutorService executor;
    private volatile boolean isRunning;
    private CacheListener cacheListener;

    public interface CacheListener {
        void onCacheProgress(String url, int percentsAvailable);
        void onCacheAvailable(String url, File file);
        void onCacheError(String url, int percentsAvailable, Exception e);
    }

    public void setCacheListener(CacheListener listener) {
        this.cacheListener = listener;
    }

    public ProxyServer(int port, Context context) {
        this.port = port;
        this.context = context;
        this.executor = Executors.newCachedThreadPool();
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress("127.0.0.1", port));
        isRunning = true;

        while (isRunning) {
            try {
                Socket socket = serverSocket.accept();
                executor.execute(new RequestHandler(socket));
            } catch (IOException e) {
                if (isRunning) {
                    Log.e(TAG, "Error accepting connection", e);
                }
            }
        }
    }

    public void stop() {
        isRunning = false;
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                Log.e(TAG, "Error closing server socket", e);
            }
        }
        executor.shutdown();
    }

    private class RequestHandler implements Runnable {
        private final Socket socket;
        private String cachedRequest;
        private String[] requestHeaders;

        RequestHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                handleRequest();
            } catch (IOException e) {
                Log.e(TAG, "Error handling request", e);
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    Log.e(TAG, "Error closing socket", e);
                }
            }
        }

        private void handleRequest() throws IOException {
            String request = readRequest(socket.getInputStream());
            if (TextUtils.isEmpty(request)) {
                Log.e(TAG, "Empty request received");
                return;
            }

            // 缓存请求头
            cachedRequest = request;
            requestHeaders = request.split("\r\n");

            String videoUrl = parseUrl(request);
            Log.d(TAG, "Received request for URL: " + videoUrl);

            if (TextUtils.isEmpty(videoUrl)) {
                Log.e(TAG, "Failed to parse video URL from request");
                return;
            }

            FileCache cache = VideoCacheManager.getInstance(context).getFileCache(videoUrl);
            if (cache.exists() && cache.length() > 0) {
                Log.d(TAG, "Using cached file for: " + videoUrl + ", length: " + cache.length());
                sendCachedResponse(cache);
            } else {
                Log.d(TAG, "Downloading and caching: " + videoUrl);
                sendAndCacheResponse(videoUrl, cache);
            }
        }

        private String readRequest(InputStream input) throws IOException {
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            StringBuilder request = new StringBuilder();
            String line;
            // Read all request headers
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    break;
                }
                request.append(line).append("\r\n");
            }
            return request.toString();
        }

        private String parseUrl(String request) {
            String[] lines = request.split("\r\n");
            if (lines.length > 0) {
                String[] parts = lines[0].split(" ");
                if (parts.length > 1) {
                    String encodedUrl = parts[1].substring(1);
                    String decodedUrl = Uri.decode(encodedUrl);
                    if (!decodedUrl.startsWith("http://") && !decodedUrl.startsWith("https://")) {
                        decodedUrl = "https://" + decodedUrl;
                    }
                    Log.d(TAG, "Parsed URL: " + decodedUrl);
                    return decodedUrl;
                }
            }
            return null;
        }

        private String getContentType(String url, String serverContentType) {
            if (serverContentType != null && !serverContentType.isEmpty()) {
                if (serverContentType.contains("webm")) {
                    return MIME_TYPE_WEBM;
                } else if (serverContentType.contains("mp4")) {
                    return MIME_TYPE_MP4;
                }
                return serverContentType;
            }

            // If the server does not provide Content Type, determine based on the file extension
            String extension = MimeTypeMap.getFileExtensionFromUrl(url);
            if (!TextUtils.isEmpty(extension)) {
                if (extension.equals("webm")) {
                    return MIME_TYPE_WEBM;
                } else if (extension.equals("mp4")) {
                    return MIME_TYPE_MP4;
                }
            }

            // Default return MP4
            return MIME_TYPE_MP4;
        }

        private void sendCachedResponse(FileCache cache) throws IOException {
            OutputStream output = null;
            try {
                output = socket.getOutputStream();

                // Check connection status
                if (socket.isClosed() || !socket.isConnected()) {
                    Log.e(TAG, "Socket is closed or not connected");
                    return;
                }

                // Get Range request header
                String rangeHeader = getRequestHeader("Range");
                long startPosition = 0;
                long endPosition = cache.length() - 1;
                long totalLength = cache.length();

                if (rangeHeader != null) {
                    try {
                        String[] ranges = rangeHeader.substring("bytes=".length()).split("-");
                        if (ranges.length > 0 && !ranges[0].isEmpty()) {
                            startPosition = Long.parseLong(ranges[0]);
                        }
                        if (ranges.length > 1 && !ranges[1].isEmpty()) {
                            endPosition = Long.parseLong(ranges[1]);
                        } else {
                            endPosition = totalLength - 1;
                        }

                        String contentType = getContentType(cache.getUrl(), null);

                        // For WebM format, if the request range exceeds the file size, return the complete file
                        if (contentType.equals(MIME_TYPE_WEBM) && startPosition >= totalLength) {
                            Log.w(TAG, "WebM: Range request out of bounds, returning full file");
                            startPosition = 0;
                            endPosition = totalLength - 1;
                        }
                        // For other formats, if the request range exceeds the file size, a 416 error is returned
                        else if (startPosition >= totalLength) {
                            Log.w(TAG, "Requested range not satisfiable: " + rangeHeader);
                            String response = "HTTP/1.1 416 Requested Range Not Satisfiable\r\n" +
                                    "Content-Range: bytes */" + totalLength + "\r\n" +
                                    "Content-Length: 0\r\n\r\n";
                            output.write(response.getBytes());
                            output.flush();
                            return;
                        }

                        // Adjust the range to ensure it is within the file size
                        startPosition = Math.max(0, startPosition);
                        endPosition = Math.min(endPosition, totalLength - 1);

                        Log.d(TAG, String.format("Adjusted range request: start=%d, end=%d, total=%d",
                                startPosition, endPosition, totalLength));
                    } catch (Exception e) {
                        Log.e(TAG, "Error parsing range header: " + rangeHeader, e);
                        startPosition = 0;
                        endPosition = totalLength - 1;
                    }
                }

                long contentLength = endPosition - startPosition + 1;
                String contentType = getContentType(cache.getUrl(), null);

                // Special handling of WebM format
                if (contentType.equals(MIME_TYPE_WEBM)) {
                    // For WebM format, always return a 206 response
                    Log.d(TAG, "WebM: Returning partial content response");
                    String contentRange = String.format("bytes %d-%d/%d", startPosition, endPosition, totalLength);
                    writePartialHeaders(output, contentLength, contentType, contentRange);
                } else {
                    // Return 206 response
                    String contentRange = String.format("bytes %d-%d/%d", startPosition, endPosition, totalLength);
                    writePartialHeaders(output, contentLength, contentType, contentRange);
                }

                try {
                    long position = startPosition;
                    byte[] buffer = new byte[BUFFER_SIZE];
                    long totalSent = 0;
                    long remainingBytes = contentLength;
                    int maxRetries = 3;
                    int retryCount = 0;

                    while (remainingBytes > 0 && retryCount < maxRetries) {
                        // Check connection status
                        if (socket.isClosed() || !socket.isConnected()) {
                            Log.w(TAG, "Connection lost while sending cached response");
                            break;
                        }

                        try {
                            int len = (int) Math.min(BUFFER_SIZE, remainingBytes);
                            byte[] data = cache.read(position, len);
                            if (data == null || data.length == 0) {
                                Log.w(TAG, "No data read from cache at position " + position);
                                break;
                            }

                            output.write(data);
                            output.flush(); // Refresh each data block immediately

                            position += data.length;
                            totalSent += data.length;
                            remainingBytes -= data.length;
                            retryCount = 0; // Reset retry count after successful sending

                        } catch (IOException e) {
                            if (e instanceof java.net.SocketException) {
                                Log.w(TAG, "Client closed connection: " + e.getMessage());
                                retryCount++;
                                if (retryCount < maxRetries) {
                                    Log.d(TAG, "Retrying send... attempt " + (retryCount + 1));
                                    Thread.sleep(100); // Wait briefly and retry
                                    continue;
                                }
                            }
                            break;
                        }
                    }

                    Log.d(TAG, String.format("Sent %d/%d bytes from cache", totalSent, contentLength));

                } catch (Exception e) {
                    Log.e(TAG, "Error sending cached response", e);
                    throw e;
                }

            } catch (Exception e) {
                Log.e(TAG, "Error sending cached response: " + e.getMessage(), e);
                if (output != null) {
                    try {
                        String errorResponse = "HTTP/1.1 500 Internal Server Error\r\n\r\n" + e.getMessage();
                        output.write(errorResponse.getBytes());
                        output.flush();
                    } catch (IOException ignored) {
                    }
                }
            }
        }

        private HttpURLConnection createConnection(URL url, String rangeHeader) throws IOException {
            HttpURLConnection connection = null;
            Exception lastException = null;

            // Try different TLS versions
            for (String tlsVersion : TLS_VERSIONS) {
                try {
                    SSLContext sslContext = SSLContext.getInstance(tlsVersion);
                    sslContext.init(null, new TrustManager[]{new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                        public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        }
                        public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        }
                    }}, new SecureRandom());

                    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());

                    connection = (HttpURLConnection) url.openConnection();
                    if (connection instanceof HttpsURLConnection) {
                        ((HttpsURLConnection) connection).setHostnameVerifier((hostname, session) -> true);
                    }

                    // Set request header
                    connection.setRequestMethod("GET");
                    connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");
                    connection.setRequestProperty("Accept", "*/*");
                    connection.setRequestProperty("Accept-Encoding", "identity");
                    connection.setRequestProperty("Connection", "keep-alive");

                    if (rangeHeader != null) {
                        connection.setRequestProperty("Range", rangeHeader);
                    }

                    connection.setInstanceFollowRedirects(true);
                    connection.setConnectTimeout(30000);
                    connection.setReadTimeout(30000);
                    connection.setDoInput(true);

                    // Record the request header before connecting
                    Map<String, List<String>> requestHeaders = connection.getRequestProperties();
                    StringBuilder headersLog = new StringBuilder("Connecting to video server with headers:");
                    for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
                        headersLog.append("\n").append(entry.getKey()).append(": ").append(entry.getValue());
                    }
                    Log.d(TAG, headersLog.toString());

                    // Attempting to connect
                    connection.connect();

                    // If the connection is successful, jump out of the loop
                    Log.d(TAG, "Successfully connected using " + tlsVersion);
                    return connection;

                } catch (Exception e) {
                    Log.w(TAG, "Failed to connect using " + tlsVersion + ": " + e.getMessage());
                    lastException = e;
                    if (connection != null) {
                        connection.disconnect();
                    }
                }
            }

            // If all attempts fail, throw the last exception
            if (lastException != null) {
                throw new IOException("Failed to establish secure connection after trying all TLS versions", lastException);
            }

            return null;
        }

        private void sendAndCacheResponse(String videoUrl, FileCache cache) throws IOException {
            HttpURLConnection connection = null;
            InputStream input = null;
            OutputStream output = null;
            long contentLength = -1;
            long totalRead = 0;

            try {
                URL url = new URL(videoUrl);
                String contentType = getContentType(videoUrl, null);
                boolean isWebM = MIME_TYPE_WEBM.equals(contentType);

                // For WebM format, do not use Range requests, always retrieve the complete file
                connection = createConnection(url, isWebM ? null : getRequestHeader("Range"));
                if (connection == null) {
                    throw new IOException("Failed to create connection");
                }

                int responseCode = connection.getResponseCode();
                Log.d(TAG, "Server response code: " + responseCode + ", message: " + connection.getResponseMessage());

                if (responseCode >= 400) {
                    handleErrorResponse(connection);
                    return;
                }

                input = connection.getInputStream();
                output = socket.getOutputStream();

                // Retrieve the content type and length returned by the server
                String serverContentType = connection.getContentType();
                contentType = getContentType(videoUrl, serverContentType);
                Log.d(TAG, "Content type: " + contentType);

                contentLength = getContentLength(connection);
                Log.d(TAG, "Content length: " + contentLength);

                // If it is in WebM format and the cache file is incomplete, delete the existing cache
                if (isWebM && cache.exists() && cache.length() != contentLength) {
                    Log.w(TAG, "Incomplete WebM cache found, deleting...");
                    cache.getCacheFile().delete();
                }

                // Processing response headers
                String rangeHeader = getRequestHeader("Range");
                long startPosition = 0;

                if (!isWebM && rangeHeader != null && responseCode == HttpURLConnection.HTTP_PARTIAL) {
                    startPosition = parseRangeHeader(rangeHeader);
                    String contentRange = connection.getHeaderField("Content-Range");
                    if (contentRange == null) {
                        contentRange = String.format("bytes %d-%d/%d", startPosition, contentLength - 1, contentLength);
                    }
                    writePartialHeaders(output, contentLength - startPosition, contentType, contentRange);
                } else {
                    writeHeaders(output, contentLength, contentType);
                }

                // Read and cache data
                byte[] buffer = new byte[BUFFER_SIZE];
                int read;
                long position = startPosition;
                long lastProgressUpdate = 0;
                long startTime = System.currentTimeMillis();
                int retryCount = 0;
                final int maxRetries = 3;

                Log.d(TAG, "Starting to read video data from position: " + position);

                // Create a buffer to store the most recently read data
                byte[] retryBuffer = new byte[BUFFER_SIZE * 2];
                int retryBufferSize = 0;
                long lastSuccessPosition = position;

                while ((read = input.read(buffer)) > 0) {
                    try {
                        // Write Cache
                        cache.write(buffer, read, position);

                        // Update retry buffer
                        if (retryBufferSize + read <= retryBuffer.length) {
                            System.arraycopy(buffer, 0, retryBuffer, retryBufferSize, read);
                            retryBufferSize = Math.min(retryBufferSize + read, retryBuffer.length);
                        } else {
                            // If the buffer is full, move the data and add new data
                            System.arraycopy(retryBuffer, read, retryBuffer, 0, retryBuffer.length - read);
                            System.arraycopy(buffer, 0, retryBuffer, retryBuffer.length - read, read);
                        }

                        // Send data to the client
                        output.write(buffer, 0, read);
                        output.flush();

                        position += read;
                        totalRead += read;
                        lastSuccessPosition = position;

                        // Update progress
                        long now = System.currentTimeMillis();
                        if (now - lastProgressUpdate >= 1000) {
                            updateProgress(videoUrl, totalRead, contentLength, startTime, now);
                            lastProgressUpdate = now;
                        }

                        // Reset retry count
                        retryCount = 0;

                    } catch (IOException e) {
                        if (e instanceof java.net.SocketException && retryCount < maxRetries) {
                            Log.w(TAG, "Client connection error, retry " + (retryCount + 1) + "/" + maxRetries);
                            retryCount++;

                            try {
                                // Wait for a while and retry
                                Thread.sleep(1000 * retryCount);

                                // Retrieve output stream again
                                output = socket.getOutputStream();

                                // If there is cached data, try resending it
                                if (retryBufferSize > 0) {
                                    output.write(retryBuffer, 0, retryBufferSize);
                                    output.flush();
                                    Log.d(TAG, "Resent " + retryBufferSize + " bytes from retry buffer");
                                }

                                // Continue the cycle
                                continue;
                            } catch (Exception retryError) {
                                Log.e(TAG, "Retry failed: " + retryError.getMessage());
                            }
                        }

                        // If the retry fails or reaches the maximum retry count, continue caching but stop sending
                        if (isWebM) {
                            Log.w(TAG, "Client disconnected, continuing to cache WebM file");
                            while ((read = input.read(buffer)) > 0) {
                                cache.write(buffer, read, position);
                                position += read;
                                totalRead += read;

                                long now = System.currentTimeMillis();
                                if (now - lastProgressUpdate >= 1000) {
                                    updateProgress(videoUrl, totalRead, contentLength, startTime, now);
                                    lastProgressUpdate = now;
                                }
                            }
                            break;
                        } else {
                            throw e;
                        }
                    }
                }

                // Verify file integrity
                if (isWebM && cache.exists()) {
                    if (cache.length() != contentLength) {
                        Log.e(TAG, "WebM file incomplete: " + cache.length() + "/" + contentLength);
                        cache.getCacheFile().delete();
                        throw new IOException("WebM file download incomplete");
                    }
                    Log.d(TAG, "WebM file downloaded completely: " + cache.length() + " bytes");
                }

                // Notify when download is complete
                if (cacheListener != null && contentLength > 0 && totalRead >= contentLength) {
                    cacheListener.onCacheAvailable(videoUrl, cache.getCacheFile());
                }

            } catch (Exception e) {
                handleException(e, videoUrl, totalRead, contentLength);
            } finally {
                closeResources(input, connection);
            }
        }

        private long getContentLength(HttpURLConnection connection) {
            String contentLengthStr = connection.getHeaderField("Content-Length");
            if (contentLengthStr != null) {
                try {
                    long length = Long.parseLong(contentLengthStr);
                    return length > 0 ? length : -1;
                } catch (NumberFormatException e) {
                    Log.w(TAG, "Invalid content length: " + contentLengthStr);
                }
            }
            return -1;
        }

        private void updateProgress(String videoUrl, long totalRead, long contentLength, long startTime, long now) {
            double progress = contentLength > 0 ? (totalRead * 100.0 / contentLength) : 0;
            double speed = totalRead * 1000.0 / (now - startTime) / 1024; // KB/s
            Log.d(TAG, String.format("Progress: %.1f%% (%d/%d bytes), Speed: %.1f KB/s",
                    progress, totalRead, contentLength, speed));

            if (cacheListener != null) {
                int percentsAvailable = contentLength > 0 ? (int) progress : (int)(totalRead / BUFFER_SIZE);
                cacheListener.onCacheProgress(videoUrl, percentsAvailable);
            }
        }

        private void handleErrorResponse(HttpURLConnection connection) throws IOException {
            String errorBody = "";
            try (InputStream errorStream = connection.getErrorStream()) {
                if (errorStream != null) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream));
                    StringBuilder errorResponse = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        errorResponse.append(line).append("\n");
                    }
                    errorBody = errorResponse.toString();
                    Log.e(TAG, "Error response body: " + errorBody);
                }
            } catch (Exception e) {
                Log.e(TAG, "Error reading error response", e);
            }
            String errorResponse = String.format("HTTP/1.1 %d %s\r\n\r\n",
                    connection.getResponseCode(), connection.getResponseMessage());
            socket.getOutputStream().write(errorResponse.getBytes());
        }

        private void handleException(Exception e, String videoUrl, long totalRead, long contentLength) {
            Log.e(TAG, "Error streaming video: " + e.getMessage(), e);
            if (cacheListener != null) {
                int progress = contentLength > 0 ? (int)((float)totalRead / contentLength * 100) : 0;
                cacheListener.onCacheError(videoUrl, progress, e);
            }
            try {
                String errorResponse = "HTTP/1.1 500 Internal Server Error\r\n\r\n" + e.getMessage();
                socket.getOutputStream().write(errorResponse.getBytes());
            } catch (IOException ignored) {
            }
        }

        private void closeResources(InputStream input, HttpURLConnection connection) {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    Log.e(TAG, "Error closing input stream", e);
                }
            }
            if (connection != null) {
                connection.disconnect();
            }
        }

        private String getRequestHeader(String headerName) {
            if (requestHeaders == null) {
                return null;
            }

            String headerNameLower = headerName.toLowerCase() + ":";
            for (String line : requestHeaders) {
                if (line.toLowerCase().startsWith(headerNameLower)) {
                    String value = line.substring(line.indexOf(':') + 1).trim();
                    Log.d(TAG, "Found header " + headerName + ": " + value);
                    return value;
                }
            }
            Log.d(TAG, "Header not found: " + headerName);
            return null;
        }

        private long parseRangeHeader(String rangeHeader) {
            try {
                // Parse Range header similar to 'bytes=1234-'
                String[] parts = rangeHeader.split("=")[1].split("-");
                return Long.parseLong(parts[0]);
            } catch (Exception e) {
                return 0;
            }
        }

        private void writePartialHeaders(OutputStream output, long contentLength, String contentType, String contentRange) throws IOException {
            StringBuilder headers = new StringBuilder();
            headers.append("HTTP/1.1 206 Partial Content\r\n");
            headers.append("Content-Type: ").append(contentType).append("\r\n");
            headers.append("Content-Length: ").append(contentLength).append("\r\n");

            if (contentRange != null && !contentRange.trim().isEmpty()) {
                headers.append("Content-Range: ").append(contentRange).append("\r\n");
            }

            headers.append("Connection: keep-alive\r\n");
            headers.append("Accept-Ranges: bytes\r\n");
            headers.append("Access-Control-Allow-Origin: *\r\n");
            headers.append("Cache-Control: no-cache\r\n");
            headers.append("\r\n");

            Log.d(TAG, "Writing partial response headers:\n" + headers.toString().trim());
            output.write(headers.toString().getBytes());
            output.flush();
        }

        private void writeHeaders(OutputStream output, long contentLength, String contentType) throws IOException {
            StringBuilder headers = new StringBuilder();
            headers.append("HTTP/1.1 200 OK\r\n");
            headers.append("Content-Type: ").append(contentType).append("\r\n");
            headers.append("Content-Length: ").append(contentLength).append("\r\n");
            headers.append("Connection: keep-alive\r\n");
            headers.append("Accept-Ranges: bytes\r\n");
            headers.append("Access-Control-Allow-Origin: *\r\n");
            headers.append("Cache-Control: no-cache\r\n");
            headers.append("\r\n");

            Log.d(TAG, "Writing response headers:\n" + headers.toString().trim());
            output.write(headers.toString().getBytes());
            output.flush();
        }
    }
}