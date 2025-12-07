package course

// 属性常量
const (
	// 第一位是1表示男性，第二位是表示是vip，第三位是1表示是周活用户
	MALE        = 1 << 0
	VIP         = 1 << 2
	WEEK_ACTIVE = 1 << 3

	// 在const中，可以用iota来实现这种从0递增的
	//MALE        = 1 << iota
	//VIP         = 1 << iota
	//WEEK_ACTIVE = 1 << iota

	// 甚至还可以
	//MALE        = 1 << iota
	//VIP
	//WEEK_ACTIVE
)

// Candidate 候选人的属性
type Candidate struct {
	Id     int
	Gender string
	Vip    bool
	Active int // 几天内活跃
	Bits   uint64
}

func (c *Candidate) SetMale() {
	c.Gender = "男"
	c.Bits |= MALE
}

func (c *Candidate) SetVip() {
	c.Vip = true
	c.Bits |= VIP
}

func (c *Candidate) SetActive(day int) {
	c.Active = day
	if day <= 7 {
		c.Bits |= WEEK_ACTIVE
	}
}

// Filter1 判断3个条件是否同时满足 常规写法，不通过位运算
func (c Candidate) Filter1(male, vip, weekActive bool) bool {
	if male && c.Gender != "男" {
		return false
	}
	if vip && !c.Vip {
		return false
	}
	if weekActive && c.Active > 7 {
		return false
	}
	return true
}

// Filter2 判断3个条件是否同时满足 位运算
func (c Candidate) Filter2(on uint64) bool {
	return c.Bits == on
}
