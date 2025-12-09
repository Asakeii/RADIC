package kvdb

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
	"log/slog"
	"os"
	"path"
)

type Badger struct {
	db   *badger.DB
	path string
}

func (b *Badger) WithDataPath(path string) *Badger {
	b.path = path
	return b
}

func (b *Badger) Open() error {
	DataDir := b.GetDbPath()
	if err := os.MkdirAll(path.Dir(DataDir), os.ModePerm); err != nil {
		return err
	}
	option := badger.DefaultOptions(DataDir).WithNumVersionsToKeep(1).WithLoggingLevel(badger.INFO)
	db, err := badger.Open(option)
	if err != nil {
		return err
	} else {
		b.db = db
		return nil
	}
}

func (b *Badger) GetDbPath() string {
	return b.path
}

// CheckAndGC 对 BadgerDB 进行 Value Log 的垃圾回收（GC），并记录回收前后的存储空间变化
func (b *Badger) CheckAndGC() {
	// 1. 获取 GC 前的大小
	lsmSize1, vlogSize1 := b.db.Size()

	// 2. 循环执行 GC
	// RunValueLogGC 会移除那些 stale 数据超过阈值（这里是 0.5 即 50%）的 vlog 文件。
	// 我们在一个循环中运行它，因为一次 GC 可能释放空间从而允许更多的 GC 发生。
	for {
		err := b.db.RunValueLogGC(0.5)
		if err == nil {
			// GC 成功了一次，继续尝试
			continue
		}
		// 如果返回 ErrNoRewrite，说明没有文件需要 GC，跳出循环
		if errors.Is(err, badger.ErrNoRewrite) {
			break
		}
		// 如果发生其他错误（例如数据库关闭），记录错误并退出
		slog.Error("badger run value log GC failed", "error", err)
		break
	}

	// 3. 获取 GC 后的大小
	lsmSize2, vlogSize2 := b.db.Size()

	// 4. 比较大小并记录日志
	if vlogSize2 < vlogSize1 {
		saved := vlogSize1 - vlogSize2
		slog.Info("badger GC completed",
			"saved_bytes", saved,
			"lsm_change", lsmSize2-lsmSize1, // 顺便记录 LSM 变化
			"vlog_before", vlogSize1,
			"vlog_after", vlogSize2,
			"lsm_size", lsmSize2,
		)
	} else {
		// 实际上如果没有回收空间，通常意味着前面 ErrNoRewrite 早就触发了 break
		// 但为了保留你的逻辑分支，这里记录未回收
		slog.Info("badger GC finished", "msg", "collect zero garbage")
	}
}

func (b *Badger) Set(k, v []byte) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	})
	return err
}

func (b *Badger) BatchSet(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return errors.New("key value not the same length")
	}
	var err error
	txn := b.db.NewTransaction(true)
	for i, key := range keys {
		value := values[i]

		if err = txn.Set(key, value); err != nil {
			_ = txn.Commit()
			txn = b.db.NewTransaction(true)
			_ = txn.Set(key, value)
		}
	}
	txn.Commit()
	return err
}
func (b *Badger) Get(k []byte) ([]byte, error) {
	var val []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		// 注意：必须使用 ValueCopy，因为 item.Value 中的切片只在闭包内有效
		val, err = item.ValueCopy(nil)
		return err
	})
	return val, err
}

func (b *Badger) BatchGet(keys [][]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))
	err := b.db.View(func(txn *badger.Txn) error {
		for i, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				// 如果某个 key 不存在，可以选择返回 error，或者在该位置留空
				// 这里选择如果找不到则返回 error，保持严格一致性
				if errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			values[i] = val
		}
		return nil
	})
	return values, err
}

// Delete 注意：这里将接收者修改为指针 *Badger 以保持一致性
func (b *Badger) Delete(k []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
}

func (b *Badger) BatchDelete(keys [][]byte) error {
	txn := b.db.NewTransaction(true)
	// 确保发生 panic 或提前返回时丢弃未提交的事务
	defer txn.Discard()

	for _, key := range keys {
		// 尝试删除
		if err := txn.Delete(key); err != nil {
			// 如果事务过大 (ErrTxnTooBig)，先提交旧的，再开新的
			if errors.Is(err, badger.ErrTxnTooBig) {
				if commitErr := txn.Commit(); commitErr != nil {
					return commitErr
				}
				txn = b.db.NewTransaction(true)
				// 在新事务中重试删除
				if err := txn.Delete(key); err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}
	// 提交最后的事务
	return txn.Commit()
}

func (b *Badger) Has(k []byte) bool {
	var exists bool
	_ = b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(k)
		if err == nil {
			exists = true
		}
		return nil // 忽略 View 的错误，只看 Get 的结果
	})
	return exists
}

func (b *Badger) IterDB(fn func(k []byte, v []byte) error) int64 {
	var count int64
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// 默认 PrefetchSize 是 100，适合遍历数据
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			// 通过 Value 回调处理值
			err := item.Value(func(v []byte) error {
				return fn(k, v)
			})
			if err != nil {
				return err // 如果用户回调返回错误，停止迭代
			}
			count++
		}
		return nil
	})

	if err != nil {
		slog.Error("IterDB stopped with error", "error", err)
	}
	return count
}

func (b *Badger) IterKey(fn func(k []byte) error) int64 {
	var count int64
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// 优化：只遍历 Key 时，设置 PrefetchValues 为 false，性能大幅提升
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if err := fn(item.Key()); err != nil {
				return err
			}
			count++
		}
		return nil
	})

	if err != nil {
		slog.Error("IterKey stopped with error", "error", err)
	}
	return count
}

func (b *Badger) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}
