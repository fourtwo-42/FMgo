package main

import (
	"FMgo/internal/config"
	"FMgo/internal/db"
	"FMgo/internal/logger"
	"FMgo/internal/model"
	"FMgo/internal/player"
	"FMgo/internal/ui"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

const Version = "0.1.2"

//go:embed radio.json
var defaultRadioConfig []byte

func main() {
	configFile := flag.String("config", "", "外部电台配置文件路径(可选)")
	version := flag.Bool("version", false, "显示版本信息")
	flag.Parse()

	if *version {
		fmt.Printf("FMgo version %s\n", Version)
		os.Exit(0)
	}

	var categories []model.Category
	var err error

	if *configFile != "" {
		// 使用外部配置文件
		data, err := os.ReadFile(*configFile)
		if err != nil {
			logger.Error("读取配置文件失败: %v", err)
			os.Exit(1)
		}
		if err := json.Unmarshal(data, &categories); err != nil {
			logger.Error("解析配置文件失败: %v", err)
			os.Exit(1)
		}
	} else {
		// 使用内置配置
		if err := json.Unmarshal(defaultRadioConfig, &categories); err != nil {
			logger.Error("解析内置配置失败: %v", err)
			os.Exit(1)
		}
	}

	config.Init()
	logger.Init()

	// Initialize database
	db, err := db.New()
	if err != nil {
		fmt.Printf("Error initializing database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Initialize player
	player, err := player.NewPlayer()
	if err != nil {
		fmt.Printf("Error initializing player: %v\n", err)
		os.Exit(1)
	}

	// Initialize UI
	ui, err := ui.New(categories, player, db)
	if err != nil {
		fmt.Printf("Error initializing UI: %v\n", err)
		os.Exit(1)
	}
	defer ui.Close()

	// Run the application
	ui.Run()
}
