package player

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"FMgo/internal/config"
	"FMgo/internal/logger"
)

const (
	// httpTimeout 单次 HTTP 请求的超时时间，避免请求卡死导致无法停止
	httpTimeout = 15 * time.Second
	// playlistFetchInterval 播放列表轮询间隔
	playlistFetchInterval = 3 * time.Second
	// retryDelay 出错后的重试间隔
	retryDelay = 3 * time.Second
	// maxSegmentQueue 待播片段队列长度上限（同时约束磁盘占用与直播延迟）
	maxSegmentQueue = 4
	// maxURLHistory 已下载片段 URL 的记忆条数，用于去重
	maxURLHistory = 200
	// maxPlaylistSize 播放列表体积上限
	maxPlaylistSize = 4 << 20
	// maxSegmentSize 单个片段体积上限
	maxSegmentSize = 8 << 20
	// maxPlaylistDepth 嵌套（多码率）播放列表的最大跟随层数
	maxPlaylistDepth = 3
)

// playCommand 启动音频播放进程，抽成变量便于测试替换。
// 使用带 context 的命令，context 取消时进程会被杀掉，不存在"停止后仍在播放"的问题。
var playCommand = func(ctx context.Context, path string) *exec.Cmd {
	return exec.CommandContext(ctx, "afplay", path)
}

// StreamPlayer 负责把 HLS 直播流下载成本地片段并顺序播放。
// 每个片段落盘为一个独立文件，播放完即删除，磁盘占用有上限。
type StreamPlayer struct {
	mu      sync.Mutex
	session *session
}

// session 表示一次播放会话，所有可变状态都在会话内，避免跨会话互相干扰。
type session struct {
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	dir      string
	client   *http.Client
	segments chan string

	// started 仅由下载协程访问
	started bool
}

func NewStreamPlayer() (*StreamPlayer, error) {
	return &StreamPlayer{}, nil
}

// PlayStream 开始播放在线音频流，立即返回，真正的下载/播放都在后台协程中进行。
func (s *StreamPlayer) PlayStream(rawURL string) error {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return errors.New("播放地址为空")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("无效的播放地址: %q", rawURL)
	}

	// 先停掉上一个会话，保证同一时刻只有一个下载/播放协程在工作
	s.Stop()

	baseDir := config.TempDir
	if baseDir == "" {
		baseDir = os.TempDir()
	}
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return fmt.Errorf("创建临时目录失败: %v", err)
	}
	dir, err := os.MkdirTemp(baseDir, "fmgo-stream-*")
	if err != nil {
		return fmt.Errorf("创建临时目录失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ses := &session{
		ctx:      ctx,
		cancel:   cancel,
		dir:      dir,
		client:   &http.Client{Timeout: httpTimeout},
		segments: make(chan string, maxSegmentQueue),
	}

	s.mu.Lock()
	s.session = ses
	s.mu.Unlock()

	ses.wg.Add(2)
	go func() {
		defer ses.wg.Done()
		ses.downloadLoop(parsed.String())
	}()
	go func() {
		defer ses.wg.Done()
		ses.playbackLoop()
	}()

	logger.Info("开始播放: %s", parsed.String())
	return nil
}

// Stop 停止当前播放并清理资源；没有会话时是空操作，可以重复调用。
func (s *StreamPlayer) Stop() {
	s.mu.Lock()
	ses := s.session
	s.session = nil
	s.mu.Unlock()
	if ses == nil {
		return
	}

	ses.cancel()
	ses.wg.Wait()

	// 清掉队列里还没来得及播放的片段
drain:
	for {
		select {
		case path := <-ses.segments:
			os.Remove(path)
		default:
			break drain
		}
	}

	if err := os.RemoveAll(ses.dir); err != nil {
		logger.Debug("清理临时目录失败: %v", err)
	}
	logger.Info("播放已停止")
}

// Cleanup 释放播放器资源，退出程序时调用。
func (s *StreamPlayer) Cleanup() {
	s.Stop()
}

func (ses *session) downloadLoop(playlistURL string) {
	seen := newURLSet(maxURLHistory)
	for {
		if ses.ctx.Err() != nil {
			return
		}

		segments, err := ses.fetchSegments(playlistURL, 0)
		if err != nil {
			if ses.ctx.Err() != nil {
				return
			}
			logger.Error("获取播放列表失败: %v", err)
			if !ses.sleep(retryDelay) {
				return
			}
			continue
		}

		var fresh []string
		for _, u := range segments {
			if seen.has(u) {
				continue
			}
			seen.add(u)
			fresh = append(fresh, u)
		}

		// 刚连上直播流时列表里通常有好几段历史内容，
		// 只播放最后一段，避免一上来就落后直播几十秒。
		if !ses.started && len(fresh) > 1 {
			fresh = fresh[len(fresh)-1:]
		}
		if len(fresh) > 0 {
			ses.started = true
		}

		for _, segmentURL := range fresh {
			if ses.ctx.Err() != nil {
				return
			}
			path, err := ses.downloadSegment(segmentURL)
			if err != nil {
				if ses.ctx.Err() != nil {
					return
				}
				logger.Error("下载片段失败: %v", err)
				continue
			}
			ses.push(path)
		}

		if !ses.sleep(playlistFetchInterval) {
			return
		}
	}
}

// push 把下载好的片段交给播放协程；队列满时丢弃最旧的一段，保证低延迟和磁盘上限。
func (ses *session) push(path string) {
	select {
	case ses.segments <- path:
		return
	default:
	}

	select {
	case stale := <-ses.segments:
		os.Remove(stale)
		logger.Debug("播放队列已满，丢弃最旧片段")
	default:
	}

	select {
	case ses.segments <- path:
	default:
		os.Remove(path)
	}
}

func (ses *session) playbackLoop() {
	for {
		select {
		case <-ses.ctx.Done():
			return
		case path := <-ses.segments:
			ses.play(path)
			os.Remove(path)
			if ses.ctx.Err() != nil {
				return
			}
		}
	}
}

func (ses *session) play(path string) {
	cmd := playCommand(ses.ctx, path)
	if err := cmd.Run(); err != nil && ses.ctx.Err() == nil {
		logger.Error("播放片段失败: %v", err)
	}
}

func (ses *session) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ses.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// fetchSegments 读取播放列表，返回可播放的媒体片段地址（绝对地址）。
// 支持相对地址、以及多码率（master）播放列表的自动跟随。
func (ses *session) fetchSegments(playlistURL string, depth int) ([]string, error) {
	req, err := http.NewRequestWithContext(ses.ctx, http.MethodGet, playlistURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := ses.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求播放列表失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("播放列表返回 %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPlaylistSize))
	if err != nil {
		return nil, fmt.Errorf("读取播放列表失败: %v", err)
	}

	base := resp.Request.URL
	var segments, variants []string
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		ref, err := base.Parse(line)
		if err != nil {
			logger.Debug("忽略无法解析的地址 %q: %v", line, err)
			continue
		}
		if ref.Scheme != "http" && ref.Scheme != "https" {
			continue
		}
		switch strings.ToLower(filepath.Ext(ref.Path)) {
		case ".m3u8", ".m3u":
			variants = append(variants, ref.String())
		case ".ts", ".m4s", ".vtt":
			// afplay 无法播放这些容器，跳过
			logger.Debug("跳过不支持的片段: %s", ref.String())
		default:
			segments = append(segments, ref.String())
		}
	}

	if len(segments) > 0 {
		return segments, nil
	}
	if len(variants) > 0 {
		if depth >= maxPlaylistDepth {
			return nil, errors.New("播放列表嵌套层级过深")
		}
		return ses.fetchSegments(variants[0], depth+1)
	}
	return nil, errors.New("播放列表中未找到可播放的媒体片段")
}

func (ses *session) downloadSegment(segmentURL string) (string, error) {
	req, err := http.NewRequestWithContext(ses.ctx, http.MethodGet, segmentURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := ses.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("请求片段失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("片段 %s 返回 %s", segmentURL, resp.Status)
	}

	file, err := os.CreateTemp(ses.dir, "seg-*")
	if err != nil {
		return "", fmt.Errorf("创建片段文件失败: %v", err)
	}
	name := file.Name()

	written, err := io.Copy(file, io.LimitReader(resp.Body, maxSegmentSize))
	if cerr := file.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(name)
		return "", fmt.Errorf("写入片段失败: %v", err)
	}
	if written == 0 {
		os.Remove(name)
		return "", errors.New("片段内容为空")
	}
	return name, nil
}

// urlSet 是一个固定容量的 URL 去重集合，超出容量时淘汰最早的记录。
type urlSet struct {
	max   int
	order []string
	items map[string]struct{}
}

func newURLSet(max int) *urlSet {
	return &urlSet{max: max, items: make(map[string]struct{}, max)}
}

func (u *urlSet) has(key string) bool {
	_, ok := u.items[key]
	return ok
}

func (u *urlSet) add(key string) {
	if _, ok := u.items[key]; ok {
		return
	}
	if len(u.order) >= u.max {
		oldest := u.order[0]
		u.order = u.order[1:]
		delete(u.items, oldest)
	}
	u.items[key] = struct{}{}
	u.order = append(u.order, key)
}
