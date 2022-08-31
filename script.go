package Redisgogogo

import "errors"

// ScriptFlush: 命令用于清除所有 Lua 脚本缓存, 如果执行没有出错，则总是返回 OK
func (r *ctxRedis) ScriptFlush() error {
	if _, err := r.do("SCRIPT", nil, "FLUSH"); err != nil {
		return err
	}

	return nil
}

// ScriptExists: 用于校验指定的脚本是否已经被保存在缓存当中。
// 返回值：一个和传入列表一一对应的返回列表，包含 0 和 1 ，前者表示脚本不存在于缓存，后者表示脚本已经在缓存里面了。
func (r *ctxRedis) ScriptExists(sha1 []string) ([]int, error) {
	params := []interface{}{"EXISTS"}
	for _, s := range sha1 {
		params = append(params, s)
	}

	rep, err := r.do("SCRIPT", redisInts, params...)
	if err != nil {
		return nil, err
	}

	return rep.([]int), nil
}

// ScripLoad：命令用于将脚本 script 添加到脚本缓存中，但并不立即执行这个脚本。
// 返回值：给定脚本的 SHA1 校验和
func (r *ctxRedis) ScriptLoad(script string) (string, error) {
	rep, err := r.do("SCRIPT", redisString, "LOAD", script)
	if err != nil {
		return "", err
	}

	return rep.(string), nil
}

// EvalSha: 根据给定的 sha1 校验码，执行缓存在服务器中的脚本。
// 参数：
//        sha1: 通过 SCRIPT LOAD 生成的 sha1 校验码。
//    keys: 在脚本中所用到的那些 Redis 键
//    args: 脚本中用到的参数
// 返回值：一个未经解析的接口，里面是lua脚本的返回
func (r *ctxRedis) EvalSha(sha1 string, keys []string, args []interface{}) (interface{}, error) {
	return r.run("EVALSHA", sha1, keys, args)
}

// Eval: 使用 Lua 解释器执行脚本。
// 参数：
//        script: 参数是一段 Lua 5.1 脚本程序。脚本不必(也不应该)定义为一个 Lua 函数。
//    keys: 在脚本中所用到的那些 Redis 键
//    args: 脚本中用到的参数
// 返回值：一个未经解析的接口，里面是lua脚本的返回
func (r *ctxRedis) Eval(script string, keys []string, args []interface{}) (interface{}, error) {
	return r.run("EVAL", script, keys, args)
}

func (r *ctxRedis) run(cmd, script string, keys []string, args []interface{}) (interface{}, error) {
	if cmd != "EVAL" && cmd != "EVALSHA" {
		return nil, errors.New("Unsupport cmd! The func only working for EVAL and EVALSHA")
	}

	lk, la := len(keys), len(args)

	// lua限制，脚本中所有参数总和不能超过8000
	// 所以 redis命令 + 脚本参数 + keys的长度参数 + keys的数量 + args的数量 <= 8000
	if 3+lk+la > 8000 {
		return nil, errors.New("Too many parameters! The sum of keys count and args count should be less than 7998")
	}

	params := make([]interface{}, 2+lk+la)
	params[0], params[1] = script, lk
	for idx, k := range keys {
		params[2+idx] = k
	}
	for idx, a := range args {
		params[2+lk+idx] = a
	}

	return r.do(cmd, nil, params...)
}
