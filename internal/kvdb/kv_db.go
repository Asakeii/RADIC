package kvdb

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// 几种常见的基于LSM-tree算法实现的KV数据库
const (
	BOLT = iota
	BADGER
)

type IKeyVakyeDB interface {
	Open() error                              // 初始化DB
	GetDbPath() string                        // 获取存储数据的目录
	Set(k, v []byte) error                    // 写入k v
	BatchSet(keys, values [][]byte) error     // 批量写入
	Get(k []byte) ([]byte, error)             // 读取key对应的value
	BatchGet(keys [][]byte) ([][]byte, error) // 批量读取， 不保证顺序
	Delete(k []byte) error                    // 删除
	BatchDelete(key [][]byte) error           // 批量删除
	Has(k []byte) bool                        // 判断某个key是否存在
	IterDB(fn func(k, v []byte) error) int64  //遍历数据库，返回数据的条数
	IterKey(fn func(k []byte) error) int64    // 遍历所有的key，返回数据条数
	Close() error                             // 把内存中的数据flush到磁盘那，同时释放文件锁
}

func GetKvDb(dbtype int, path string) (IKeyVakyeDB, error) {
	paths := strings.Split(path, "/")
	parentPath := strings.Join(paths[0:len(paths)-1], "/") // 父路径

	info, err := os.Stat(parentPath)
	if os.IsNotExist(err) {
		// 如果父路径不存在
		slog.Info("父目录不存在，自动创建",
			slog.String("path", parentPath),
			slog.String("mode", "0o600"))
		os.MkdirAll(parentPath, 0o600) // 用0o表示八进制，其实只有0也可以，只不过不明显
	} else {
		if info.Mode().IsRegular() {
			// 如果父路径是个普通文件，则把他删掉
			// 写日志
			os.Remove(parentPath)
			slog.Warn("父路径本该是目录，但却是一个文件，尝试删除并重建",
				slog.String("path", parentPath),
				slog.String("file_size", strconv.FormatInt(info.Size(), 10)),
				slog.Time("file_mtime", info.ModTime()))
		}
	}

	var db IKeyVakyeDB
	switch dbtype {
	case BADGER:
		db = new(Badger)
	default:
		db = new(Bolt)

	}
	err = db.Open()
	return db, err

}
