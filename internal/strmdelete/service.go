package strmdelete

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"litepan/internal/domain"
	"litepan/internal/eventbus"
	"litepan/internal/file"
	"litepan/internal/strm"
	"litepan/pkg/safego"
)

const (
	configKey  = "strm_delete_linkage_config"
	pendingKey = "strm_delete_linkage_pending"

	StrategyBlock   = "block"
	StrategyConfirm = "confirm"
)

// TaskConfig 是单个 STRM 任务的删除联动设置：一个任务一份，互不影响。
type TaskConfig struct {
	TaskID       int64  `json:"task_id"`
	Threshold    int    `json:"threshold"`
	Strategy     string `json:"strategy"`
	DelayMinutes int    `json:"delay_minutes"`
}

type Config struct {
	Enabled bool         `json:"enabled"`
	Items   []TaskConfig `json:"items"`
}

type TaskOption struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	LocalDir string `json:"local_dir"`
}

type Pending struct {
	ID        int64     `json:"id"`
	TaskID    int64     `json:"task_id"`
	TaskName  string    `json:"task_name"`
	AccountID int64     `json:"account_id"`
	Relative  string    `json:"relative_path"`
	RemoteID  string    `json:"remote_id"`
	ParentID  string    `json:"parent_id"`
	CreatedAt time.Time `json:"created_at"`
}

type Status struct {
	Config  Config       `json:"config"`
	Tasks   []TaskOption `json:"tasks"`
	Pending []Pending    `json:"pending"`
}

type Options struct {
	Configs domain.ConfigRepository
	Tasks   domain.StrmTaskRepository
	Files   *file.Service
	Strm    *strm.Service
	StrmDir string
	Bus     *eventbus.Bus
	Log     *slog.Logger
}

type watchDir struct {
	taskID int64
	root   string
}

type candidate struct {
	taskID int64
	path   string
	isDir  bool
}

type candidateBatch struct {
	generation uint64
	items      map[string]candidate
	timer      *time.Timer
}

type Service struct {
	configs domain.ConfigRepository
	tasks   domain.StrmTaskRepository
	files   *file.Service
	strm    *strm.Service
	strmDir string
	bus     *eventbus.Bus
	log     *slog.Logger

	mu      sync.Mutex
	config  Config
	pending []Pending
	watcher *fsnotify.Watcher
	watched map[string]watchDir
	batches map[int64]*candidateBatch
	// warnedRoots 记录「已经告警过无法监听」的任务根目录，避免每分钟重复刷日志
	warnedRoots map[int64]string
	cancel      context.CancelFunc
}

func New(opts Options) *Service {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		configs:     opts.Configs,
		tasks:       opts.Tasks,
		files:       opts.Files,
		strm:        opts.Strm,
		strmDir:     opts.StrmDir,
		bus:         opts.Bus,
		log:         log,
		config:      defaultConfig(),
		watched:     make(map[string]watchDir),
		batches:     make(map[int64]*candidateBatch),
		warnedRoots: make(map[int64]string),
	}
}

const (
	defaultThreshold    = 100
	immediateSettleTime = 2 * time.Second
)

func defaultConfig() Config {
	return Config{Items: []TaskConfig{}}
}

func normalizeConfig(cfg Config) (Config, error) {
	seen := make(map[int64]struct{}, len(cfg.Items))
	items := make([]TaskConfig, 0, len(cfg.Items))
	for _, item := range cfg.Items {
		if item.TaskID <= 0 {
			continue
		}
		if _, ok := seen[item.TaskID]; ok {
			continue
		}
		norm, err := normalizeTaskConfig(item)
		if err != nil {
			return cfg, err
		}
		seen[item.TaskID] = struct{}{}
		items = append(items, norm)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].TaskID < items[j].TaskID })
	cfg.Items = items
	if cfg.Enabled && len(items) == 0 {
		return cfg, domain.Errorf(domain.CodeValidation, "请至少选择一个 STRM 任务")
	}
	return cfg, nil
}

func normalizeTaskConfig(item TaskConfig) (TaskConfig, error) {
	// 没填的字段按默认值补齐，前端漏传也不会把保护参数打成 0
	if item.Threshold == 0 {
		item.Threshold = defaultThreshold
	}
	if item.Strategy == "" {
		item.Strategy = StrategyConfirm
	}
	if item.Threshold < 1 || item.Threshold > 100000 {
		return item, domain.Errorf(domain.CodeValidation, "保护阈值应在 1～100000 之间")
	}
	if item.DelayMinutes < 0 || item.DelayMinutes > 1440 {
		return item, domain.Errorf(domain.CodeValidation, "延迟时间应在 0～1440 分钟之间")
	}
	if item.Strategy != StrategyBlock && item.Strategy != StrategyConfirm {
		return item, domain.Errorf(domain.CodeValidation, "未知的超限处理方式")
	}
	return item, nil
}

// taskConfig 取某个任务当前的设置；没配置过的任务返回 false。
func (s *Service) taskConfig(taskID int64) (TaskConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range s.config.Items {
		if item.TaskID == taskID {
			return item, true
		}
	}
	return TaskConfig{}, false
}

func (s *Service) Start(ctx context.Context) {
	if s == nil || s.configs == nil || s.tasks == nil || s.files == nil || s.strm == nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		cancel()
		return
	}
	s.cancel = cancel
	s.mu.Unlock()
	if err := s.load(runCtx); err != nil {
		s.log.Warn("加载 STRM 删除联动配置失败", "err", err)
	}
	go safego.Guard(s.log, "strm-delete.watch", func() { s.run(runCtx) })
}

func (s *Service) load(ctx context.Context) error {
	cfg := defaultConfig()
	if raw, ok, err := s.configs.Get(ctx, configKey); err != nil {
		return err
	} else if ok && strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			return err
		}
	}
	var err error
	if cfg, err = normalizeConfig(cfg); err != nil {
		return err
	}
	var pending []Pending
	if raw, ok, err := s.configs.Get(ctx, pendingKey); err != nil {
		return err
	} else if ok && strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &pending); err != nil {
			return err
		}
	}
	if pending == nil {
		pending = []Pending{}
	}
	s.mu.Lock()
	s.config = cfg
	s.pending = pending
	s.mu.Unlock()
	return nil
}

func (s *Service) run(ctx context.Context) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		s.log.Error("启动 STRM 删除监听失败", "err", err)
		return
	}
	defer watcher.Close()
	s.mu.Lock()
	s.watcher = watcher
	s.mu.Unlock()
	s.reconcile(ctx)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcile(ctx)
		case err := <-watcher.Errors:
			if err != nil {
				s.log.Warn("STRM 删除监听异常，本轮不执行远端删除", "err", err)
			}
		case evt := <-watcher.Events:
			s.handleEvent(ctx, evt)
		}
	}
}

func (s *Service) selectedTasks(ctx context.Context) map[int64]*domain.StrmTask {
	s.mu.Lock()
	cfg := s.config
	s.mu.Unlock()
	out := make(map[int64]*domain.StrmTask)
	if !cfg.Enabled {
		return out
	}
	selected := make(map[int64]struct{}, len(cfg.Items))
	for _, item := range cfg.Items {
		selected[item.TaskID] = struct{}{}
	}
	tasks, err := s.tasks.List(ctx)
	if err != nil {
		s.log.Warn("读取 STRM 删除监听任务失败", "err", err)
		return out
	}
	for _, task := range tasks {
		if _, ok := selected[task.ID]; ok {
			out[task.ID] = task
		}
	}
	return out
}

func (s *Service) reconcile(ctx context.Context) {
	tasks := s.selectedTasks(ctx)
	desiredRoots := make(map[int64]string, len(tasks))
	for id, task := range tasks {
		desiredRoots[id] = filepath.Clean(strm.TaskOutputDir(s.strmDir, strm.TaskRelDir(task.GroupDir, task.OutputFolder)))
	}
	s.mu.Lock()
	watcher := s.watcher
	if watcher == nil {
		s.mu.Unlock()
		return
	}
	for path, info := range s.watched {
		if root, ok := desiredRoots[info.taskID]; !ok || root != info.root {
			_ = watcher.Remove(path)
			delete(s.watched, path)
		}
	}
	s.mu.Unlock()
	for id, root := range desiredRoots {
		if st, err := os.Stat(root); err != nil || !st.IsDir() {
			s.warnUnwatchableRoot(id, root, err)
			continue
		}
		s.clearUnwatchableRoot(id)
		s.addTree(id, root, root)
	}
}

// warnUnwatchableRoot 对同一个任务的同一个根目录只告警一次，避免每分钟刷屏。
func (s *Service) warnUnwatchableRoot(taskID int64, root string, err error) {
	s.mu.Lock()
	if s.warnedRoots == nil {
		s.warnedRoots = map[int64]string{}
	}
	already := s.warnedRoots[taskID] == root
	s.warnedRoots[taskID] = root
	s.mu.Unlock()
	if already {
		return
	}
	reason := "目录不存在或不是目录"
	if err != nil {
		reason = err.Error()
	}
	s.log.Warn("STRM 删除联动未能监听任务目录，该任务不会触发远端删除",
		"task_id", taskID, "dir", root, "reason", reason)
}

func (s *Service) clearUnwatchableRoot(taskID int64) {
	s.mu.Lock()
	delete(s.warnedRoots, taskID)
	s.mu.Unlock()
}

func (s *Service) addTree(taskID int64, root, start string) {
	_ = filepath.WalkDir(start, func(path string, entry os.DirEntry, err error) error {
		if err != nil || !entry.IsDir() {
			return nil
		}
		s.mu.Lock()
		_, exists := s.watched[path]
		if !exists && s.watcher != nil {
			if addErr := s.watcher.Add(path); addErr == nil {
				s.watched[path] = watchDir{taskID: taskID, root: root}
			} else {
				s.log.Warn("监听 STRM 目录失败", "path", path, "err", addErr)
			}
		}
		s.mu.Unlock()
		return nil
	})
}

func (s *Service) handleEvent(ctx context.Context, evt fsnotify.Event) {
	s.mu.Lock()
	info, knownDir := s.watched[evt.Name]
	if !knownDir {
		info = s.watched[filepath.Dir(evt.Name)]
	}
	s.mu.Unlock()
	if info.taskID == 0 {
		return
	}
	s.log.Debug("STRM 删除监听收到文件事件", "task_id", info.taskID, "op", evt.Op.String(), "path", evt.Name)
	if evt.Op&fsnotify.Create != 0 {
		if st, err := os.Stat(evt.Name); err == nil && st.IsDir() {
			s.addTree(info.taskID, info.root, evt.Name)
		}
		return
	}
	if evt.Op&(fsnotify.Remove|fsnotify.Rename) == 0 || evt.Name == info.root {
		return
	}
	if s.strm.IsTaskBusy(info.taskID) {
		return
	}
	isDir := knownDir
	if !isDir && !strings.EqualFold(filepath.Ext(evt.Name), ".strm") {
		return
	}
	if knownDir {
		s.mu.Lock()
		for path := range s.watched {
			if path == evt.Name || strings.HasPrefix(path, evt.Name+string(filepath.Separator)) {
				_ = s.watcher.Remove(path)
				delete(s.watched, path)
			}
		}
		s.mu.Unlock()
	}
	s.queue(ctx, candidate{taskID: info.taskID, path: evt.Name, isDir: isDir})
}

func (s *Service) queue(ctx context.Context, c candidate) {
	c.path = filepath.Clean(c.path)
	s.mu.Lock()
	var item TaskConfig
	found := false
	if s.config.Enabled {
		for _, configured := range s.config.Items {
			if configured.TaskID == c.taskID {
				item = configured
				found = true
				break
			}
		}
	}
	if !found {
		s.mu.Unlock()
		return
	}
	batch := s.batches[c.taskID]
	if batch == nil {
		batch = &candidateBatch{items: make(map[string]candidate)}
		s.batches[c.taskID] = batch
	}
	mergeCandidate(batch.items, c)
	batch.generation++
	generation := batch.generation
	if batch.timer != nil {
		batch.timer.Stop()
	}
	delay := taskDelay(item.DelayMinutes)
	batch.timer = time.AfterFunc(delay, func() {
		safego.Guard(s.log, "strm-delete.process", func() { s.processBatch(ctx, c.taskID, generation) })
	})
	s.mu.Unlock()
}

func taskDelay(minutes int) time.Duration {
	if minutes <= 0 {
		return immediateSettleTime
	}
	return time.Duration(minutes) * time.Minute
}

func mergeCandidate(items map[string]candidate, incoming candidate) {
	for path, existing := range items {
		if pathContains(path, incoming.path) {
			if path == incoming.path && incoming.isDir {
				existing.isDir = true
				items[path] = existing
			}
			return
		}
		if pathContains(incoming.path, path) {
			delete(items, path)
		}
	}
	items[incoming.path] = incoming
}

func pathContains(parent, child string) bool {
	return child == parent || strings.HasPrefix(child, parent+string(filepath.Separator))
}

func (s *Service) processBatch(ctx context.Context, taskID int64, generation uint64) {
	s.mu.Lock()
	batch := s.batches[taskID]
	if batch == nil || batch.generation != generation {
		s.mu.Unlock()
		return
	}
	delete(s.batches, taskID)
	items := make([]candidate, 0, len(batch.items))
	for _, item := range batch.items {
		items = append(items, item)
	}
	s.mu.Unlock()
	sort.Slice(items, func(i, j int) bool { return items[i].path < items[j].path })
	for _, item := range items {
		if ctx.Err() != nil {
			return
		}
		s.process(ctx, item)
	}
}

func (s *Service) process(ctx context.Context, c candidate) {
	if _, err := os.Stat(c.path); err == nil || !os.IsNotExist(err) || s.strm.IsTaskBusy(c.taskID) {
		return
	}
	s.mu.Lock()
	enabled := s.config.Enabled
	s.mu.Unlock()
	if !enabled {
		return
	}
	cfg, ok := s.taskConfig(c.taskID)
	if !ok {
		return
	}
	task, err := s.tasks.Get(ctx, c.taskID)
	if err != nil {
		return
	}
	root := filepath.Clean(strm.TaskOutputDir(s.strmDir, strm.TaskRelDir(task.GroupDir, task.OutputFolder)))
	rel, err := filepath.Rel(root, c.path)
	if err != nil || rel == "." || rel == "" || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return
	}
	rel = filepath.ToSlash(rel)
	target, parentID, err := s.resolveTarget(ctx, task, rel, c.isDir)
	if err != nil {
		s.log.Warn("STRM 删除联动未找到唯一远端目标，已跳过", "task_id", task.ID, "path", rel, "err", err)
		return
	}
	count := 1
	if c.isDir {
		count, err = s.countMedia(ctx, task, target.ID, cfg.Threshold)
		if err != nil {
			s.log.Warn("STRM 删除联动统计失败，已跳过远端删除", "task_id", task.ID, "path", rel, "err", err)
			return
		}
	}
	if count > cfg.Threshold {
		if cfg.Strategy == StrategyConfirm {
			s.addPending(ctx, task, rel, target.ID, parentID, cfg.Threshold)
		} else {
			s.notify(ctx, "warning", "STRM 删除已拦截", fmt.Sprintf("%s 的媒体文件数量已超过保护阈值 %d，未删除网盘源文件。", rel, cfg.Threshold), task.AccountID, 0)
		}
		return
	}
	if err := s.files.DeleteFiles(ctx, task.AccountID, []string{target.ID}, parentID); err != nil {
		s.log.Warn("STRM 删除联动执行失败", "task_id", task.ID, "path", rel, "err", err)
		return
	}
	s.log.Info("STRM 删除已联动远端", "task_id", task.ID, "path", rel, "media_count", count)
}

func (s *Service) resolveTarget(ctx context.Context, task *domain.StrmTask, rel string, isDir bool) (*domain.FileItem, string, error) {
	parts := strings.Split(strings.Trim(rel, "/"), "/")
	parentID := task.ParentID
	for i, name := range parts {
		items, err := s.files.List(ctx, task.AccountID, parentID, true)
		if err != nil {
			return nil, "", err
		}
		last := i == len(parts)-1
		var matches []domain.FileItem
		for _, item := range items {
			matched := targetNameMatches(item, name, last, isDir)
			if matched && (!last || item.IsDir == isDir) {
				matches = append(matches, item)
			}
		}
		if len(matches) != 1 {
			return nil, "", domain.Errorf(domain.CodeNotFound, "路径不存在或不唯一：%s", strings.Join(parts[:i+1], "/"))
		}
		if last {
			item := matches[0]
			return &item, parentID, nil
		}
		parentID = matches[0].ID
	}
	return nil, "", domain.Errorf(domain.CodeNotFound, "远端目标不存在")
}

func targetNameMatches(item domain.FileItem, localName string, last, targetIsDir bool) bool {
	if last && item.IsDir != targetIsDir {
		return false
	}
	if last && !targetIsDir {
		localStem := strings.TrimSuffix(localName, ".strm")
		remoteStem := strings.TrimSuffix(item.Name, filepath.Ext(item.Name))
		return strings.EqualFold(remoteStem, localStem) || strings.EqualFold(strm.SafeStem(remoteStem), localStem)
	}
	return item.Name == localName || strm.SafeName(item.Name) == localName
}

func (s *Service) countMedia(ctx context.Context, task *domain.StrmTask, rootID string, limit int) (int, error) {
	queue := []string{rootID}
	count := 0
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		items, err := s.files.List(ctx, task.AccountID, id, true)
		if err != nil {
			return 0, err
		}
		for _, item := range items {
			if item.IsDir {
				queue = append(queue, item.ID)
				continue
			}
			if s.strm.IsTaskMediaFile(task, item.Name) {
				count++
				if count > limit {
					return count, nil
				}
			}
		}
	}
	return count, nil
}

func (s *Service) addPending(ctx context.Context, task *domain.StrmTask, rel, remoteID, parentID string, threshold int) {
	s.mu.Lock()
	for _, item := range s.pending {
		if item.TaskID == task.ID && item.Relative == rel {
			s.mu.Unlock()
			return
		}
	}
	id := time.Now().UnixMilli()
	for _, existing := range s.pending {
		if existing.ID >= id {
			id = existing.ID + 1
		}
	}
	item := Pending{ID: id, TaskID: task.ID, TaskName: task.Name, AccountID: task.AccountID, Relative: rel, RemoteID: remoteID, ParentID: parentID, CreatedAt: time.Now()}
	s.pending = append(s.pending, item)
	_ = s.savePendingLocked(ctx)
	s.mu.Unlock()
	s.notify(ctx, "warning", "STRM 删除等待确认", fmt.Sprintf("%s 的媒体文件数量已超过保护阈值 %d。请确认是否删除网盘源文件。", rel, threshold), task.AccountID, id)
}

func (s *Service) notify(ctx context.Context, level, title, message string, accountID, refID int64) {
	if s.bus == nil {
		return
	}
	s.bus.Publish(ctx, eventbus.NotificationCreated{Level: level, Category: domain.NotificationCategoryStrmDeleteConfirm, Title: title, Message: message, AccountID: accountID, RefID: refID})
}

func (s *Service) savePendingLocked(ctx context.Context) error {
	raw, err := json.Marshal(s.pending)
	if err != nil {
		return err
	}
	return s.configs.Set(ctx, pendingKey, string(raw))
}

func (s *Service) Status(ctx context.Context) (Status, error) {
	tasks, err := s.tasks.List(ctx)
	if err != nil {
		return Status{}, err
	}
	options := make([]TaskOption, 0, len(tasks))
	for _, task := range tasks {
		options = append(options, TaskOption{ID: task.ID, Name: task.Name, LocalDir: strm.TaskRelDir(task.GroupDir, task.OutputFolder)})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := append([]Pending{}, s.pending...)
	return Status{Config: s.config, Tasks: options, Pending: pending}, nil
}

func (s *Service) UpdateConfig(ctx context.Context, cfg Config) (Status, error) {
	norm, err := normalizeConfig(cfg)
	if err != nil {
		return Status{}, err
	}
	raw, err := json.Marshal(norm)
	if err != nil {
		return Status{}, err
	}
	if err := s.configs.Set(ctx, configKey, string(raw)); err != nil {
		return Status{}, err
	}
	s.mu.Lock()
	s.config = norm
	selected := make(map[int64]struct{}, len(norm.Items))
	for _, item := range norm.Items {
		selected[item.TaskID] = struct{}{}
	}
	for taskID, batch := range s.batches {
		if _, ok := selected[taskID]; !norm.Enabled || !ok {
			if batch.timer != nil {
				batch.timer.Stop()
			}
			delete(s.batches, taskID)
		}
	}
	s.mu.Unlock()
	s.reconcile(ctx)
	return s.Status(ctx)
}

func (s *Service) Confirm(ctx context.Context, id int64) error {
	s.mu.Lock()
	var item *Pending
	for i := range s.pending {
		if s.pending[i].ID == id {
			copy := s.pending[i]
			item = &copy
			break
		}
	}
	s.mu.Unlock()
	if item == nil {
		return domain.Errorf(domain.CodeNotFound, "待确认删除记录不存在")
	}
	task, err := s.tasks.Get(ctx, item.TaskID)
	if err != nil {
		return err
	}
	local := strm.TaskOutputDir(s.strmDir, filepath.FromSlash(filepath.Join(strm.TaskRelDir(task.GroupDir, task.OutputFolder), item.Relative)))
	if _, err := os.Stat(local); err == nil || !os.IsNotExist(err) {
		return domain.Errorf(domain.CodeValidation, "本地路径已经恢复，已停止删除")
	}
	target, parentID, err := s.resolveTarget(ctx, task, item.Relative, true)
	if err != nil || target.ID != item.RemoteID || parentID != item.ParentID {
		return domain.Errorf(domain.CodeValidation, "远端路径已变化，请取消后重新触发")
	}
	if err := s.files.DeleteFiles(ctx, item.AccountID, []string{item.RemoteID}, item.ParentID); err != nil {
		return err
	}
	return s.removePending(ctx, id)
}

func (s *Service) Cancel(ctx context.Context, id int64) error { return s.removePending(ctx, id) }

func (s *Service) removePending(ctx context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.pending {
		if s.pending[i].ID == id {
			s.pending = append(s.pending[:i], s.pending[i+1:]...)
			return s.savePendingLocked(ctx)
		}
	}
	return domain.Errorf(domain.CodeNotFound, "待确认删除记录不存在")
}
