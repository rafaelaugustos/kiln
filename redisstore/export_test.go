package redisstore

var Library = library

func Plain(s *Store) {
	s.plain.Store(true)
}
