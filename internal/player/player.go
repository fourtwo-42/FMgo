package player

import (
	"errors"
	"fmt"
	"sync"

	"FMgo/internal/logger"
)

// Player 是播放器的主控制器，负责管理音频流的播放
type Player struct {
	mu           sync.Mutex
	streamPlayer *StreamPlayer
	isPlaying    bool
	currentURL   string
}

// NewPlayer 创建一个新的播放器实例
func NewPlayer() (*Player, error) {
	logger.Info("初始化播放器")
	streamPlayer, err := NewStreamPlayer()
	if err != nil {
		logger.Error("创建流播放器失败: %v", err)
		return nil, fmt.Errorf("创建流播放器失败: %v", err)
	}

	return &Player{streamPlayer: streamPlayer}, nil
}

// Play 开始播放指定的URL
func (p *Player) Play(rawURL string) error {
	if rawURL == "" {
		return errors.New("播放地址为空")
	}

	p.mu.Lock()
	if p.isPlaying && p.currentURL == rawURL {
		p.mu.Unlock()
		logger.Info("已经在播放该URL")
		return nil
	}
	p.mu.Unlock()

	// PlayStream 内部会先停掉上一个播放会话
	if err := p.streamPlayer.PlayStream(rawURL); err != nil {
		p.mu.Lock()
		p.isPlaying = false
		p.currentURL = ""
		p.mu.Unlock()
		logger.Error("开始播放失败: %v", err)
		return fmt.Errorf("开始播放失败: %v", err)
	}

	p.mu.Lock()
	p.isPlaying = true
	p.currentURL = rawURL
	p.mu.Unlock()
	return nil
}

// Stop 停止当前播放（没有在播放时也可以安全调用）
func (p *Player) Stop() {
	p.streamPlayer.Stop()

	p.mu.Lock()
	p.isPlaying = false
	p.currentURL = ""
	p.mu.Unlock()
}

// IsPlaying 返回当前是否正在播放
func (p *Player) IsPlaying() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.isPlaying
}

// CurrentURL 返回当前正在播放的URL
func (p *Player) CurrentURL() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.currentURL
}

// Cleanup 清理播放器资源
func (p *Player) Cleanup() {
	p.Stop()
}
