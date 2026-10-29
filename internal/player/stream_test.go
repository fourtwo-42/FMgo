package player

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"FMgo/internal/config"
)

// fakePlayer 记录被播放的片段，并模拟一个"一直播到被杀掉"的播放器进程。
type fakePlayer struct {
	mu    sync.Mutex
	paths []string
	log   string
}

func installFakePlayer(t *testing.T) *fakePlayer {
	t.Helper()
	fp := &fakePlayer{log: t.TempDir() + "/play.log"}
	orig := playCommand
	playCommand = func(ctx context.Context, path string) *exec.Cmd {
		fp.mu.Lock()
		fp.paths = append(fp.paths, path)
		fp.mu.Unlock()
		// $1 是片段路径；写出后再 sleep，直到 context 取消时被 kill
		script := fmt.Sprintf("echo $1 >> %s; sleep 30", fp.log)
		return exec.CommandContext(ctx, "/bin/sh", "-c", script, "afplay", path)
	}
	t.Cleanup(func() { playCommand = orig })
	return fp
}

func (f *fakePlayer) played() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paths...)
}

func (f *fakePlayer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.paths)
}

// newTestServer 起一个模拟的 HLS 直播源，每轮返回新的片段地址。
func newTestServer(t *testing.T, segmentDelay time.Duration) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	seq := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			mu.Lock()
			seq++
			start := seq
			mu.Unlock()
			fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:7\n")
			for i := 0; i < 5; i++ {
				fmt.Fprintf(w, "#EXTINF:7,\n/seg-%d.aac\n", start*10+i)
			}
			return
		}
		if segmentDelay > 0 {
			time.Sleep(segmentDelay)
		}
		w.Write(make([]byte, 2048))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// TestParsePlaylistRelativeAndMaster 覆盖：相对地址解析 + 多码率播放列表跟随。
func TestParsePlaylistRelativeAndMaster(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=64000\n/variant/index.m3u8\n")
	})
	mux.HandleFunc("/variant/index.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXTINF:7,\n../seg1.aac\nseg2.aac\n")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ses := &session{
		ctx:    context.Background(),
		client: &http.Client{Timeout: httpTimeout},
	}
	segments, err := ses.fetchSegments(srv.URL+"/master.m3u8", 0)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	want := []string{srv.URL + "/seg1.aac", srv.URL + "/variant/seg2.aac"}
	if len(segments) != len(want) {
		t.Fatalf("片段数量不符: got %v want %v", segments, want)
	}
	for i := range want {
		if segments[i] != want[i] {
			t.Errorf("片段 %d: got %s want %s", i, segments[i], want[i])
		}
	}
}

// TestPlayStreamPlaysSegments 覆盖：正常播放（下载 → 顺序播放 → 用后即删）。
func TestPlayStreamPlaysSegments(t *testing.T) {
	config.TempDir = t.TempDir()
	fp := installFakePlayer(t)
	srv := newTestServer(t, 0)

	s, err := NewStreamPlayer()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PlayStream(srv.URL + "/live.m3u8"); err != nil {
		t.Fatalf("PlayStream 失败: %v", err)
	}

	if !waitFor(t, 5*time.Second, func() bool { return fp.count() > 0 }) {
		s.Stop()
		t.Fatal("超时：没有开始播放任何片段")
	}

	// 播放过的片段文件应被删除，临时目录里最多剩队列中的几片
	s.mu.Lock()
	dir := s.session.dir
	s.mu.Unlock()
	if !waitFor(t, 5*time.Second, func() bool { return countFiles(t, dir) <= maxSegmentQueue+1 }) {
		t.Errorf("片段文件数量没有收敛，当前 %d", countFiles(t, dir))
	}

	s.Stop()
	played := fp.count()
	if played == 0 {
		t.Fatal("没有播放记录")
	}

	// 停止后不应再启动新的播放（幽灵播放回归测试）
	time.Sleep(500 * time.Millisecond)
	if fp.count() != played {
		t.Errorf("Stop 之后仍在启动播放: %d -> %d", played, fp.count())
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("Stop 之后临时目录未清理: %s", dir)
	}
}

// TestStopBeforeFirstSegment 复现原来的幽灵播放问题：
// 缓冲还没就绪时 Stop，之后绝不能再启动 afplay。
func TestStopBeforeFirstSegment(t *testing.T) {
	config.TempDir = t.TempDir()
	fp := installFakePlayer(t)
	srv := newTestServer(t, 300*time.Millisecond) // 慢速网络，保证 Stop 时还没下载完

	s, err := NewStreamPlayer()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PlayStream(srv.URL + "/live.m3u8"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	s.Stop()

	time.Sleep(1200 * time.Millisecond)
	if n := fp.count(); n != 0 {
		t.Fatalf("Stop 之后仍然启动了播放（幽灵播放）：%d 次", n)
	}
}

// TestQueueIsBounded 覆盖：磁盘占用有上限，直播延迟不会无限增长。
func TestQueueIsBounded(t *testing.T) {
	config.TempDir = t.TempDir()
	installFakePlayer(t) // 假播放器很慢（sleep 30），下载速度远大于播放速度
	srv := newTestServer(t, 0)

	s, err := NewStreamPlayer()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PlayStream(srv.URL + "/live.m3u8"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	s.mu.Lock()
	dir := s.session.dir
	s.mu.Unlock()

	waitFor(t, 3*time.Second, func() bool { return countFiles(t, dir) >= maxSegmentQueue })
	if n := countFiles(t, dir); n > maxSegmentQueue+1 {
		t.Errorf("队列/磁盘未受限，片段文件数 = %d", n)
	}
}

// TestStopIsIdempotent 覆盖：重复 Stop / 重复播放不会 panic、不会泄漏协程。
func TestStopIsIdempotent(t *testing.T) {
	config.TempDir = t.TempDir()
	installFakePlayer(t)
	srv := newTestServer(t, 0)

	s, err := NewStreamPlayer()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.PlayStream(srv.URL + "/live.m3u8"); err != nil {
			t.Fatal(err)
		}
		s.Stop()
		s.Stop()
	}

	// 重启后仍能正常工作
	if err := s.PlayStream(srv.URL + "/live.m3u8"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	s.Stop()
}

// TestInvalidURL 覆盖：空地址 / 非法协议应直接报错而不是启动协程。
func TestInvalidURL(t *testing.T) {
	s, _ := NewStreamPlayer()
	for _, u := range []string{"", "   ", "file:///etc/passwd", "not a url"} {
		if err := s.PlayStream(u); err == nil {
			t.Errorf("地址 %q 应该报错", u)
		}
	}
}

func countFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	return len(entries)
}
