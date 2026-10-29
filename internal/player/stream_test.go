package player

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"FMgo/internal/config"
)

// fakePlayer 记录被播放的分片文件，并模拟一个"跟着文件增长一直播到被杀掉"的 afplay 进程。
type fakePlayer struct {
	mu    sync.Mutex
	paths []string
	sizes []int64
	log   string
}

func installFakePlayer(t *testing.T) *fakePlayer {
	t.Helper()
	fp := &fakePlayer{log: t.TempDir() + "/play.log"}
	orig := playCommand
	playCommand = func(ctx context.Context, path string) *exec.Cmd {
		var size int64
		if fi, err := os.Stat(path); err == nil {
			size = fi.Size()
		}
		fp.mu.Lock()
		fp.paths = append(fp.paths, path)
		fp.sizes = append(fp.sizes, size)
		fp.mu.Unlock()

		// 模拟 afplay：文件持续增长就一直"播"下去，连续 3s 不再增长才退出（相当于读到 EOF）。
		script := fmt.Sprintf(`
echo "$1" >> %s
last=-1; idle=0
while [ "$idle" -lt 30 ]; do
  cur=$(stat -f%%z "$1" 2>/dev/null || echo 0)
  if [ "$cur" = "$last" ]; then idle=$((idle+1)); else idle=0; last=$cur; fi
  sleep 0.1
done
`, fp.log)
		return exec.CommandContext(ctx, "/bin/sh", "-c", script, "afplay", path)
	}
	t.Cleanup(func() { playCommand = orig })
	return fp
}

func (f *fakePlayer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.paths)
}

func (f *fakePlayer) firstSize() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sizes) == 0 {
		return 0
	}
	return f.sizes[0]
}

func (f *fakePlayer) firstPath() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.paths) == 0 {
		return ""
	}
	return f.paths[0]
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

func countFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	return len(entries)
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

// TestAppendKeepsOneGrowingFile 覆盖核心思路：所有片段都追加到同一个文件，
// 播放端只用一个 afplay 跟着这个文件读，才能真正连续。
func TestAppendKeepsOneGrowingFile(t *testing.T) {
	ses := &session{ctx: context.Background(), dir: t.TempDir()}
	part, err := ses.newPart()
	if err != nil {
		t.Fatal(err)
	}
	ses.part = part

	var want bytes.Buffer
	for i := 0; i < 3; i++ {
		src := filepath.Join(ses.dir, fmt.Sprintf("src-%d", i))
		chunk := bytes.Repeat([]byte{byte('A' + i)}, 100)
		if err := os.WriteFile(src, chunk, 0644); err != nil {
			t.Fatal(err)
		}
		if err := ses.appendSegment(src); err != nil {
			t.Fatalf("追加第 %d 段失败: %v", i, err)
		}
		want.Write(chunk)
	}

	if ses.part != part {
		t.Fatal("应该始终写同一个文件")
	}
	if part.segments != 3 || part.size != 300 {
		t.Errorf("统计不对: segments=%d size=%d", part.segments, part.size)
	}
	got, err := os.ReadFile(part.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Error("同一个文件里的内容和按顺序追加的片段不一致")
	}
}

// TestStartWaitsForLeadBuffer 覆盖：起播前先写好前瞻缓冲，
// 否则 afplay 读到文件末尾就会退出（原版"只播一小段"的根因）。
func TestStartWaitsForLeadBuffer(t *testing.T) {
	config.TempDir = t.TempDir()
	fp := installFakePlayer(t)
	srv := newTestServer(t, 0) // 每段 2048 字节

	s, err := NewStreamPlayer()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PlayStream(srv.URL + "/live.m3u8"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	if !waitFor(t, 5*time.Second, func() bool { return fp.count() > 0 }) {
		t.Fatal("超时：没有开始播放")
	}
	const segSize = 2048
	if min := int64(leadSegments * segSize); fp.firstSize() < min {
		t.Errorf("起播时缓冲不足：%d 字节 < %d（每段 %d 字节）", fp.firstSize(), min, segSize)
	}
}

// TestPlaybackFollowsGrowingFile 覆盖：播放期间数据持续追加到同一个文件，
// 且不会为每个片段重新启动一个播放进程。
func TestPlaybackFollowsGrowingFile(t *testing.T) {
	config.TempDir = t.TempDir()
	fp := installFakePlayer(t)
	srv := newTestServer(t, 0)

	s, err := NewStreamPlayer()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PlayStream(srv.URL + "/live.m3u8"); err != nil {
		t.Fatal(err)
	}

	if !waitFor(t, 5*time.Second, func() bool { return fp.count() > 0 }) {
		s.Stop()
		t.Fatal("超时：没有开始播放")
	}
	path := fp.firstPath()
	sizeBefore := fp.firstSize()

	// 服务端每轮都会产出新片段，播放中的文件应该继续变大。
	if !waitFor(t, 3*time.Second, func() bool {
		fi, err := os.Stat(path)
		return err == nil && fi.Size() > sizeBefore
	}) {
		s.Stop()
		t.Fatalf("播放期间缓冲文件没有继续增长（停在 %d 字节）", sizeBefore)
	}

	// 同一个文件不应该被拆成多个 afplay 进程。
	if got := fp.count(); got != 1 {
		s.Stop()
		t.Fatalf("连续播放期间不应重启播放进程，实际启动 %d 次", got)
	}

	// 停止后不应再启动新的播放（幽灵播放回归测试）
	s.Stop()
	played := fp.count()
	time.Sleep(400 * time.Millisecond)
	if fp.count() != played {
		t.Errorf("Stop 之后仍在启动播放: %d -> %d", played, fp.count())
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

	// 临时目录应被清空
	s.mu.Lock()
	session := s.session
	s.mu.Unlock()
	if session != nil {
		t.Fatal("Stop 后 session 未清空")
	}
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

// TestStartNewPartWhenCaughtUp 覆盖：afplay 读到写头退出后，换一个新文件继续写，
// 避免把已经播过的内容从头再播一遍。
func TestStartNewPartWhenCaughtUp(t *testing.T) {
	ses := &session{ctx: context.Background(), dir: t.TempDir()}
	p0, err := ses.newPart()
	if err != nil {
		t.Fatal(err)
	}
	ses.part = p0

	if !ses.startNewPart(p0) {
		t.Fatal("startNewPart 不应结束会话")
	}
	if ses.part == p0 {
		t.Fatal("应换成一个全新的文件")
	}
	if _, err := os.Stat(p0.path); !os.IsNotExist(err) {
		t.Error("追平的旧文件应被删除")
	}
	if ses.part == nil || ses.part.file == nil {
		t.Error("新文件应该是可写状态")
	}
}
