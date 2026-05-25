package tern

import (
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

const progressTableKeySep = "\x00"

func progressTableKey(namespace, table string) string {
	return namespace + progressTableKeySep + table
}

func indexEngineTableProgress(tables []engine.TableProgress) map[string]*engine.TableProgress {
	index := make(map[string]*engine.TableProgress, len(tables))
	for i := range tables {
		tp := &tables[i]
		index[progressTableKey(tp.Namespace, tp.Table)] = tp
	}
	return index
}

func engineProgressForTask(index map[string]*engine.TableProgress, task *storage.Task) (*engine.TableProgress, bool) {
	tp, ok := index[progressTableKey(task.Namespace, task.TableName)]
	return tp, ok
}

func indexProtoTableProgress(tables []*ternv1.TableProgress) map[string]*ternv1.TableProgress {
	index := make(map[string]*ternv1.TableProgress, len(tables))
	for _, tp := range tables {
		if tp == nil {
			continue
		}
		index[progressTableKey(tp.Namespace, tp.TableName)] = tp
	}
	return index
}

func protoProgressForTask(index map[string]*ternv1.TableProgress, task *storage.Task) (*ternv1.TableProgress, bool) {
	tp, ok := index[progressTableKey(task.Namespace, task.TableName)]
	return tp, ok
}
