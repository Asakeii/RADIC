package test

import (
	"RADIC/course"
	"testing"
)

func TestCandidate_Filters(t *testing.T) {
	// 定义测试用例结构
	type testCase struct {
		name string
		// 候选人的初始状态
		setupCandidate func() course.Candidate
		// Filter1 的输入
		reqMale, reqVip, reqActive bool
		// Filter2 的输入
		reqBits uint64
		// Filter1 的预期结果
		wantFilter1 bool
		// Filter2 的预期结果
		wantFilter2 bool
	}

	tests := []testCase{
		{
			name: "完全匹配：候选人全能 vs 要求全能",
			setupCandidate: func() course.Candidate {
				c := course.Candidate{Id: 1}
				c.SetMale()
				c.SetVip()
				c.SetActive(3) // 3天内活跃
				return c
			},
			reqMale: true, reqVip: true, reqActive: true,
			reqBits:     course.MALE | course.VIP | course.WEEK_ACTIVE,
			wantFilter1: true,
			wantFilter2: true, // 属性完全一致 (13 == 13)
		},
		{
			name: "完全不匹配：候选人为空 vs 要求男性",
			setupCandidate: func() course.Candidate {
				return course.Candidate{Id: 2} // 没有任何属性
			},
			reqMale: true, reqVip: false, reqActive: false,
			reqBits:     course.MALE,
			wantFilter1: false,
			wantFilter2: false,
		},
		{
			name: "部分匹配（差异点）：候选人是男VIP vs 只要求男性",
			setupCandidate: func() course.Candidate {
				c := course.Candidate{Id: 3}
				c.SetMale()
				c.SetVip() // 多了一个 VIP 属性
				return c
			},
			// Filter1 逻辑：只要你是男的就行，你是VIP我不关心 -> 通过
			reqMale: true, reqVip: false, reqActive: false,
			// Filter2 逻辑：你的属性(5) 必须等于 MALE(1) -> 失败
			reqBits:     course.MALE,
			wantFilter1: true,
			wantFilter2: false, // 注意这里：Filter2 因为严格相等而失败
		},
		{
			name: "活跃度测试：活跃时间超标",
			setupCandidate: func() course.Candidate {
				c := course.Candidate{Id: 4}
				c.SetMale()
				c.SetActive(10) // 10天不满足周活
				return c
			},
			reqMale: true, reqVip: false, reqActive: true,
			reqBits:     course.MALE | course.WEEK_ACTIVE,
			wantFilter1: false, // 要求周活，但实际是10天 -> 失败
			wantFilter2: false, // Bits里没有 WEEK_ACTIVE 位 -> 失败
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.setupCandidate()

			// 测试 Filter1
			got1 := c.Filter1(tc.reqMale, tc.reqVip, tc.reqActive)
			if got1 != tc.wantFilter1 {
				t.Errorf("Filter1() got = %v, want %v", got1, tc.wantFilter1)
			}

			// 测试 Filter2
			got2 := c.Filter2(tc.reqBits)
			if got2 != tc.wantFilter2 {
				t.Errorf("Filter2() got = %v, want %v (Bits: %b, Req: %b)",
					got2, tc.wantFilter2, c.Bits, tc.reqBits)
			}
		})
	}
}

// 为了防止编译器把函数调用优化掉（Dead Code Elimination），我们需要把结果赋给一个全局变量
var Result bool

func BenchmarkFilter1_Bool(b *testing.B) {
	// 1. 准备数据
	c := course.Candidate{Id: 1}
	c.SetMale()
	c.SetVip()
	c.SetActive(3)

	// 重置计时器，排除掉上面准备数据的时间
	b.ResetTimer()

	// 2. 循环 b.N 次
	for i := 0; i < b.N; i++ {
		// 我们模拟最复杂的场景：检查3个条件
		Result = c.Filter1(true, true, true)
	}
}

func BenchmarkFilter2_Bit(b *testing.B) {
	// 1. 准备数据
	c := course.Candidate{Id: 1}
	c.SetMale()
	c.SetVip()
	c.SetActive(3)

	// 提前计算好掩码，避免在循环里重复计算（虽然位运算计算极快，但我们只测Filter）
	reqBits := uint64(course.MALE | course.VIP | course.WEEK_ACTIVE)

	b.ResetTimer()

	// 2. 循环 b.N 次
	for i := 0; i < b.N; i++ {
		Result = c.Filter2(reqBits)
	}
}
