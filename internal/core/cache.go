package core

func ForgetLeaderboard(id string) {
	lbMu.Lock()
	defer lbMu.Unlock()
	delete(Leaderboards, id)
}

func ForgetAllLeaderboards() {
	lbMu.Lock()
	defer lbMu.Unlock()
	Leaderboards = make(map[string]*Leaderboard)
}
