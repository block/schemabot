package tern

import (
	"github.com/block/schemabot/pkg/engine"
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
