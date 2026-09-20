package strm

import (
	"testing"

	"litepan/internal/domain"
)

func TestIsTaskMediaFileUsesEffectiveExtensions(t *testing.T) {
	service := NewService(ServiceOptions{})
	if !service.IsTaskMediaFile(&domain.StrmTask{}, "影片.mkv") {
		t.Fatal("任务未填写扩展名时应使用 STRM 默认媒体扩展名")
	}
	if service.IsTaskMediaFile(&domain.StrmTask{}, "poster.jpg") {
		t.Fatal("图片不应计入媒体文件数量")
	}
	task := &domain.StrmTask{Extensions: "iso"}
	if !service.IsTaskMediaFile(task, "原盘.iso") || service.IsTaskMediaFile(task, "影片.mkv") {
		t.Fatal("任务自定义扩展名应覆盖默认值")
	}
}
