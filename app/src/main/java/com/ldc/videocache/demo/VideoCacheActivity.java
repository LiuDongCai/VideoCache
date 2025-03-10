package com.ldc.videocache.demo;

import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import android.widget.Button;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;

import com.google.android.exoplayer2.ExoPlayer;
import com.google.android.exoplayer2.MediaItem;
import com.google.android.exoplayer2.PlaybackException;
import com.google.android.exoplayer2.Player;
import com.google.android.exoplayer2.source.hls.HlsMediaSource;
import com.google.android.exoplayer2.ui.StyledPlayerView;
import com.google.android.exoplayer2.upstream.DefaultDataSourceFactory;
import com.ldc.videocache.M3U8Cache;
import com.ldc.videocache.VideoCacheManager;

import java.io.File;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class VideoCacheActivity extends AppCompatActivity {
    private static final String TAG = "VideoCacheActivity";
    private TextView statusText;
    private ExoPlayer player;
    private StyledPlayerView playerView;
    private M3U8Cache m3u8Cache;
    private VideoCacheManager cacheManager;
    private boolean isM3u8Caching = false;
    private static final int UPDATE_INTERVAL = 200; // Update every 200ms for smoother playback

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_video_cache);

        statusText = findViewById(R.id.statusText);
        playerView = findViewById(R.id.playerView);
        Button playMp4Button = findViewById(R.id.playMp4Button);
        Button playM3u8Button = findViewById(R.id.playM3u8Button);
        
        // 初始化缓存管理器
        cacheManager = VideoCacheManager.getInstance(this);
        
        // 初始化 ExoPlayer
        player = new ExoPlayer.Builder(this).build();
        playerView.setPlayer(player);
        
        // 设置播放器监听器
        player.addListener(new Player.Listener() {
            @Override
            public void onPlayerError(PlaybackException error) {
                String errorMessage = "播放错误: " + error.getMessage();
                Log.e(TAG, errorMessage, error);
                statusText.setText(errorMessage);
            }

            @Override
            public void onPlaybackStateChanged(int state) {
                switch (state) {
                    case Player.STATE_BUFFERING:
                        statusText.setText("正在缓冲...");
                        break;
                    case Player.STATE_READY:
                        statusText.setText("准备就绪，开始播放");
                        break;
                    case Player.STATE_ENDED:
                        statusText.setText("播放完成");
                        break;
                    case Player.STATE_IDLE:
                        statusText.setText("播放器空闲");
                        break;
                }
            }
        });

        // MP4视频示例
        playMp4Button.setOnClickListener(v -> playMp4Video());

        // M3U8视频示例
        playM3u8Button.setOnClickListener(v -> startVideoCache());

    }

    private void playMp4Video() {
        try {
            // 重置播放器状态
            player.stop();
            player.clearMediaItems();
            
            // 示例MP4视频URL
//            String mp4Url = "http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4";
//            String mp4Url = "https://videos.pexels.com/video-files/17895933/17895933-hd_1920_1080_50fps.mp4";
//            String mp4Url = "https://videos.pexels.com/video-files/30877811/13202973_1080_1920_30fps.mp4";
            String mp4Url = "https://videos.pexels.com/video-files/1350205/1350205-hd_1920_1080_30fps.mp4";
//            String mp4Url = "https://github.com/mdn/html-examples/raw/main/link-rel-preload/video/sintel-short.webm";

            // 获取代理URL并设置缓存监听
            String proxyUrl = cacheManager.getProxyUrl(mp4Url);
            cacheManager.registerCacheListener(new VideoCacheManager.CacheListener() {
                @Override
                public void onCacheProgress(String url, int percentsAvailable) {
                    runOnUiThread(() -> {
                        String progress = String.format("缓存进度: %d%%", percentsAvailable);
                        if (statusText.getText().toString().startsWith("正在缓冲")) {
                            statusText.setText(progress + " (缓冲中...)");
                        } else if (statusText.getText().toString().startsWith("准备就绪")) {
                            statusText.setText(progress + " (播放中)");
                        } else {
                            statusText.setText(progress);
                        }
                    });
                }

                @Override
                public void onCacheAvailable(String url, File file) {
                    Log.d(TAG, "缓存完成: " + file.getAbsolutePath());
                }

                @Override
                public void onCacheError(String url, int percentsAvailable, Exception e) {
                    Log.e(TAG, "缓存错误: " + e.getMessage());
                    runOnUiThread(() -> {
                        statusText.setText("缓存错误: " + e.getMessage());
                    });
                }
            }, mp4Url);

            Log.d(TAG, "原始URL: " + mp4Url);
            Log.d(TAG, "代理URL: " + proxyUrl);
            statusText.setText("正在加载MP4视频...");

            // 创建 MediaItem
            MediaItem mediaItem = MediaItem.fromUri(proxyUrl);
            
            // 设置到播放器并开始播放
            player.setMediaItem(mediaItem);

            player.prepare();
            player.play();
            
        } catch (Exception e) {
            Log.e(TAG, "播放MP4视频出错: " + e.getMessage(), e);
            statusText.setText("播放出错: " + e.getMessage());
        }
    }

    private void startVideoCache() {
        // 停止当前播放
        player.stop();
        player.clearMediaItems();

//        String m3u8Url = "https://video.591.com.tw/online/target/hls/union/2023/05/08/pc/1683516699736-859624-476554.m3u8";
//        String m3u8Url = "https://video.591.com.tw/online/target/house/hls/2024-07-04/1038836/master.m3u8";
        String m3u8Url = "https://video.591.com.tw/debug/target/hls/union/2025/01/13/mobile/8756-183050-884672.m3u8";
        startM3u8Cache(m3u8Url);
    }

    private void startM3u8Cache(String url) {
        File cacheDir = new File(getExternalCacheDir(), "video-cache");
        m3u8Cache = new M3U8Cache(url, cacheDir);
        isM3u8Caching = true;
        
        m3u8Cache.setCacheListener(new M3U8Cache.CacheListener() {
            @Override
            public void onProgress(int completed, int total, int failed) {
                runOnUiThread(() -> {
                    String progress = String.format("Caching: %d/%d (Failed: %d)", completed, total, failed);
                    statusText.setText(progress);
                });
            }

            @Override
            public void onError(String error) {
                runOnUiThread(() -> {
                    statusText.setText("Error: " + error);
                });
            }

            @Override
            public void onComplete(boolean success, String localPath) {
                isM3u8Caching = false;
                if (success) {
                    runOnUiThread(() -> {
                        statusText.setText("Cache completed");
                    });
                }
            }

            @Override
            public void onReadyForPlayback(String localPath) {
                runOnUiThread(() -> {
                    statusText.setText("Starting playback while caching...");
                    startPlayback(localPath);
                });

                // 开始定期更新M3U8文件
                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        if (isM3u8Caching && m3u8Cache != null && !m3u8Cache.isCompleted()) {
                            m3u8Cache.updatePartialM3U8();
                        }else{
                            timer.cancel();
                        }
                    }
                }, 0, UPDATE_INTERVAL);

            }
        });
        
        new Thread(() -> m3u8Cache.cache(url)).start();
    }
    
    private void startPlayback(String localPath) {
        try {
            File m3u8File = new File(localPath);
            if (!m3u8File.exists() || !m3u8File.canRead()) {
                throw new IOException("M3U8 file not accessible: " + localPath);
            }
            
            Log.d(TAG, "Playing local M3U8 file: " + localPath);
            
            // 创建 HLS media source
            HlsMediaSource mediaSource = new HlsMediaSource.Factory(
                new DefaultDataSourceFactory(this, "ldcdemo-agent"))
                .createMediaSource(MediaItem.fromUri(Uri.fromFile(m3u8File)));
            
            // 设置播放器
            player.setMediaSource(mediaSource);
            player.prepare();
            
            player.setPlayWhenReady(true);
            
        } catch (Exception e) {
            Log.e(TAG, "Error starting playback", e);
            statusText.setText("Error starting playback: " + e.getMessage());
        }
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        if (m3u8Cache != null) {
            m3u8Cache.cancel();
        }
        if (player != null) {
            player.release();
        }
        // 注销所有缓存监听
        cacheManager.unregisterAllCacheListeners();
        cacheManager.release();
    }

    @Override
    public void onResume() {
        super.onResume();
        if (player != null) {
            player.setPlayWhenReady(true);
        }
    }

    @Override
    public void onPause() {
        super.onPause();
        if (player != null) {
            player.setPlayWhenReady(false);
        }
    }
} 