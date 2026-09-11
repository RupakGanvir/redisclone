package server

// handlerFunc is the signature every command implements. It writes its
// reply directly to c and returns an error only for protocol-level
// problems (wrong arity, wrong type, bad integer, ...) — those get turned
// into a RESP error reply by dispatch().
type handlerFunc func(s *Server, c *clientConn, args []string) error

type command struct {
	fn handlerFunc
	// isWrite marks commands that mutate the keyspace. Only these get
	// appended to the AOF — logging read commands would just bloat the
	// file with no benefit, since replay only needs to reconstruct state.
	isWrite bool
}

var commandTable = map[string]command{
	// Connection / server
	"PING": {cmdPing, false},
	"ECHO": {cmdEcho, false},

	// Generic
	"DEL":      {cmdDel, true},
	"EXISTS":   {cmdExists, false},
	"TYPE":     {cmdType, false},
	"KEYS":     {cmdKeys, false},
	"FLUSHALL": {cmdFlushAll, true},
	"DBSIZE":   {cmdDBSize, false},
	"EXPIRE":   {cmdExpire, true},
	"TTL":      {cmdTTL, false},
	"PERSIST":  {cmdPersist, true},

	// Strings
	"GET":    {cmdGet, false},
	"SET":    {cmdSet, true},
	"INCR":   {cmdIncr, true},
	"INCRBY": {cmdIncrBy, true},
	"APPEND": {cmdAppend, true},
	"STRLEN": {cmdStrLen, false},

	// Lists
	"LPUSH":  {cmdLPush, true},
	"RPUSH":  {cmdRPush, true},
	"LPOP":   {cmdLPop, true},
	"RPOP":   {cmdRPop, true},
	"LRANGE": {cmdLRange, false},
	"LLEN":   {cmdLLen, false},

	// Hashes
	"HSET":    {cmdHSet, true},
	"HGET":    {cmdHGet, false},
	"HDEL":    {cmdHDel, true},
	"HGETALL": {cmdHGetAll, false},
	"HEXISTS": {cmdHExists, false},
	"HLEN":    {cmdHLen, false},

	// Sets
	"SADD":      {cmdSAdd, true},
	"SREM":      {cmdSRem, true},
	"SMEMBERS":  {cmdSMembers, false},
	"SISMEMBER": {cmdSIsMember, false},
	"SCARD":     {cmdSCard, false},

	// Pub/Sub
	"SUBSCRIBE":   {cmdSubscribe, false},
	"UNSUBSCRIBE": {cmdUnsubscribe, false},
	"PUBLISH":     {cmdPublish, false},
}
