package types

// ToString 将倒排索引中的value的两项：field和word合并成一个字符串
func (kw *Keyword) ToString() string {
	if len(kw.Word) > 0 {
		return kw.Field + "\001" + kw.Word
	} else {
		return ""
	}
}
