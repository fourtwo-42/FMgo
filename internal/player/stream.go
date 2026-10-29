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
	// playlistFetchInterval 播放列表轮询间隔。
	// 直播片段大约每 7s 产出一个，轮询必须明显快于片段周期，
	// 否则新片段要等下一轮才被发现，写缓冲就会断流。
	playlistFetchInterval = 1 * time.Second
	// retryDelay 出错后的重试间隔
	retryDelay = 2 * time.Second
	// leadSegments 写缓冲领先播放的片段数。
	// afplay 读到文件当前末尾就会退出，所以必须提前把后续片段写进缓冲文件，
	// 让它一直有内容可读；3 段（≈21s）足以吸收轮询/下载抖动。
	leadSegments = 3
	// leadWaitTimeout 起播前等待缓冲的最长时间，超时则用已有片段先播。
	leadWaitTimeout = 5 * time.Second
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

// StreamPlayer 负责把 HLS 直播流下载成本地 AAC 缓冲文件，再用一个 afplay 进程连续播放。
type StreamPlayer struct {
	mu      sync.Mutex
	session *session
}

// bufferPart 是一段连续的 AAC 缓冲文件。
// 下载协程只管往文件末尾追加片段；播放协程用一个 afplay 顺序读取，
// afplay 会跟着文件增长一直播下去，因此不会像"每段起一个进程"那样每隔几秒断一次。
type bufferPart struct {
	path     string
	file     *os.File
	size     int64
	segments int
}

func (p *bufferPart) close() error {
	if p.file == nil {
		return nil
	}
	err := p.file.Close()
	p.file = nil
	return err
}

// session 表示一次播放会话，所有可变状态都在会话内，避免跨会话互相干扰。
type session struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	dir    string
	client *http.Client

	mu sync.Mutex
	// part 是当前正在追加/播放的缓冲文件。
	// 跟上一个版本一样：始终写同一个文件，afplay 跟着它一直读。
	part *bufferPart
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
		ctx:    ctx,
		cancel: cancel,
		dir:    dir,
		client: &http.Client{Timeout: httpTimeout},
	}

	part, err := ses.newPart()
	if err != nil {
		cancel()
		os.RemoveAll(dir)
		return fmt.Errorf("创建缓冲文件失败: %v", err)
	}
	ses.part = part

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

	ses.mu.Lock()
	if ses.part != nil {
		ses.part.close()
		ses.part = nil
	}
	ses.mu.Unlock()

	if err := os.RemoveAll(ses.dir); err != nil {
		logger.Debug("清理临时目录失败: %v", err)
	}
	logger.Info("播放已停止")
}

// Cleanup 释放播放器资源，退出程序时调用。
func (s *StreamPlayer) Cleanup() {
	s.Stop()
}

// newPart 创建一个新的空缓冲文件。
func (ses *session) newPart() (*bufferPart, error) {
	file, err := os.CreateTemp(ses.dir, "part-*")
	if err != nil {
		return nil, err
	}
	return &bufferPart{path: file.Name(), file: file}, nil
}

func (ses *session) downloadLoop(playlistURL string) {
	seen := newURLSet(maxURLHistory)
	first := true
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

		// 刚连上直播流时列表里通常有好几段历史内容。
		// 不要把整个窗口都写进缓冲（那样一上来就落后几十秒），
		// 只保留最新 leadSegments 段作为前瞻缓冲，之后靠轮询持续追加。
		if first && len(fresh) > leadSegments {
			fresh = fresh[len(fresh)-leadSegments:]
		}
		if len(fresh) > 0 {
			first = false
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
			err = ses.appendSegment(path)
			os.Remove(path)
			if err != nil {
				if ses.ctx.Err() != nil {
					return
				}
				logger.Error("写入缓冲文件失败: %v", err)
			}
		}

		if !ses.sleep(playlistFetchInterval) {
			return
		}
	}
}

// appendSegment 把一个下载好的片段追加到当前缓冲文件末尾。
// 跟上一个版本一样：始终写同一个文件，afplay 跟着它一直读。
func (ses *session) appendSegment(path string) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()

	part := ses.part
	if part == nil || part.file == nil {
		return errors.New("缓冲文件已关闭")
	}

	src, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("打开片段失败: %v", err)
	}
	defer src.Close()

	written, err := io.Copy(part.file, src)
	if err != nil {
		return fmt.Errorf("追加片段失败: %v", err)
	}
	part.size += written
	part.segments++
	return nil
}

func (ses *session) playbackLoop() {
	for {
		part, ok := ses.waitForBuffered()
		if !ok {
			return
		}

		// 单个 afplay 进程持续读取缓冲文件；文件增长时它会一直跟着播。
		ses.play(part.path)
		if ses.ctx.Err() != nil {
			return
		}

		// afplay 退出说明已经读到写头（下载跟不上）。换一个新文件继续写，
		// 避免把已经播过的内容从头再播一遍。
		if !ses.startNewPart(part) {
			return
		}
	}
}

// waitForBuffered 等待缓冲文件攒够前瞻段数再返回，避免 afplay 很快读到写头而退出。
func (ses *session) waitForBuffered() (*bufferPart, bool) {
	deadline := time.Now().Add(leadWaitTimeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		ses.mu.Lock()
		part := ses.part
		if part == nil {
			ses.mu.Unlock()
			return nil, false
		}
		ready := part.segments >= leadSegments
		empty := part.segments == 0
		ses.mu.Unlock()

		if ready || (!empty && time.Now().After(deadline)) {
			return part, true
		}

		select {
		case <-ses.ctx.Done():
			return nil, false
		case <-ticker.C:
		}
	}
}

// startNewPart 在 afplay 读到头退出后，换一个新的空缓冲文件继续写。
func (ses *session) startNewPart(played *bufferPart) bool {
	if ses.ctx.Err() != nil {
		return false
	}

	ses.mu.Lock()
	if ses.part != played {
		// 已经被换过了
		ses.mu.Unlock()
		return true
	}
	played.close()
	next, err := ses.newPart()
	if err != nil {
		ses.mu.Unlock()
		logger.Error("创建缓冲文件失败: %v", err)
		return false
	}
	ses.part = next
	ses.mu.Unlock()

	os.Remove(played.path)
	logger.Debug("播放追上了写入位置，切换新的缓冲文件")
	return true
}

func (ses *session) play(path string) {
	cmd := playCommand(ses.ctx, path)
	if err := cmd.Run(); err != nil && ses.ctx.Err() == nil {
		logger.Error("播放失败: %v", err)
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
