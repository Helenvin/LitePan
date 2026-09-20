package strmdelete

import (
	"path/filepath"
	"testing"
	"time"
)

func TestMergeCandidateKeepsHighestDeletedPath(t *testing.T) {
	taskID := int64(1)
	root := filepath.Join("strm", "动漫剧", "海贼王 (1999)")
	season := filepath.Join(root, "Season 1")
	items := map[string]candidate{}

	mergeCandidate(items, candidate{taskID: taskID, path: filepath.Join(season, "S01E01.strm")})
	mergeCandidate(items, candidate{taskID: taskID, path: filepath.Join(season, "S01E02.strm")})
	mergeCandidate(items, candidate{taskID: taskID, path: season, isDir: true})
	mergeCandidate(items, candidate{taskID: taskID, path: filepath.Join(root, "Season 2", "S02E01.strm")})
	mergeCandidate(items, candidate{taskID: taskID, path: root, isDir: true})

	if len(items) != 1 {
		t.Fatalf("候选数量 = %d，期望只保留作品目录", len(items))
	}
	got, ok := items[root]
	if !ok || !got.isDir {
		t.Fatalf("未保留作品目录候选：%+v", items)
	}

	mergeCandidate(items, candidate{taskID: taskID, path: filepath.Join(root, "Season 3", "S03E01.strm")})
	if len(items) != 1 {
		t.Fatalf("作品目录已入队后不应再加入子文件：%+v", items)
	}
}

func TestTaskDelayKeepsShortSettleWindowForImmediateMode(t *testing.T) {
	if got := taskDelay(0); got != 2*time.Second {
		t.Fatalf("立即处理的归并时间 = %s，期望 2s", got)
	}
	if got := taskDelay(10); got != 10*time.Minute {
		t.Fatalf("延迟处理时间 = %s，期望 10m", got)
	}
}

func TestNormalizeTaskConfigAllowsImmediateMode(t *testing.T) {
	got, err := normalizeTaskConfig(TaskConfig{TaskID: 1, Threshold: 10, Strategy: StrategyConfirm, DelayMinutes: 0})
	if err != nil {
		t.Fatalf("立即处理配置被拒绝：%v", err)
	}
	if got.DelayMinutes != 0 {
		t.Fatalf("延迟分钟 = %d，期望保留 0", got.DelayMinutes)
	}
}
