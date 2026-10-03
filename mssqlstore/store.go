package mssqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/rafaelaugustos/kiln/driver"
)

var (
	_ driver.Store       = (*Store)(nil)
	_ driver.Notifier    = (*Store)(nil)
	_ driver.Transactor  = (*Store)(nil)
	_ driver.LimitReader = (*Store)(nil)
	_ driver.TxWriter    = (*TxWriter)(nil)
)

// Store is a [driver.Store] on SQL Server. It also implements [driver.Transactor] and
// [driver.LimitReader], and [driver.Notifier] through the bus given with [Bus]. It is safe for
// concurrent use.
type Store struct {
	db     *sql.DB
	prefix string
	q      statements
	bus    driver.Bus
	nt     *notifier

	mu      sync.Mutex
	cursors struct {
		parent   int64
		awaiting int64
		limit    string
		holder   []byte
	}
}

// New returns a store on db. Unless [NoMigrate] is given, New first migrates the tables as
// [Migrate] does; with NoMigrate it only checks them and fails when a migration or change of this
// release is missing. Either way it fails when the tables' migration number is newer than this
// release knows. Writes made through a [TxWriter] also use connections of their own, so New fails
// with [driver.ErrInvalid] when db allows a single open connection.
func New(ctx context.Context, db *sql.DB, opts ...Option) (*Store, error) {
	c := newConfig(opts)
	if !validPrefix(c.prefix) {
		return nil, fmt.Errorf("%w: prefix %q", driver.ErrInvalid, c.prefix)
	}
	if db.Stats().MaxOpenConnections == 1 {
		return nil, fmt.Errorf("%w: mssqlstore needs at least two open connections", driver.ErrInvalid)
	}
	var err error
	if c.noMigrate {
		err = checkSchema(ctx, db, c.prefix)
	} else {
		err = migrate(ctx, db, c.prefix)
	}
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, prefix: c.prefix, q: newStatements(c.prefix), bus: c.bus}
	if c.bus != nil {
		s.nt = newNotifier(c.bus)
	}
	return s, nil
}

// Close publishes the events still pending, when the store has a bus. It closes neither db nor
// the bus. Call it after nothing uses the store any more; calling it again does nothing.
func (s *Store) Close() {
	s.nt.close()
}

// Tx returns a writer that inserts jobs and opens and seals batches inside tx, so that they
// commit or roll back with the application's work. tx must be open on the store's database, at
// READ COMMITTED. A nil tx gives a writer whose writes fail with [driver.ErrNilTx].
func (s *Store) Tx(tx *sql.Tx) *TxWriter {
	return &TxWriter{s: s, tx: tx}
}

type statements struct {
	account           string
	active            string
	advance           string
	allocate          string
	archive           string
	archivedParents   string
	attach            string
	awaiting          string
	batch             string
	batches           string
	busy              string
	cancel            string
	claim             string
	claimUniques      string
	completeBatches   string
	count             string
	counts            string
	createRecurring   string
	declare           string
	declared          string
	directives        string
	dropFailed        string
	dropHolders       string
	dueRecurring      string
	ensure            string
	expiredFailed     string
	finishedBatchDeps string
	fire              string
	fired             string
	hasRecurring      string
	heartbeat         string
	holderPage        string
	idleBatches       string
	insertDeps        string
	insertDoomed      string
	insertJobs        string
	insertPlain       string
	job               string
	lead              string
	limitInfo         string
	limitPage         string
	lockArchived      string
	lockBatch         string
	lockBatches       string
	lockChildren      string
	lockFailed        string
	lockLimits        string
	lockParents       string
	lockRunning       string
	lockTargets       string
	nextDue           string
	openBatch         string
	orphans           string
	pause             string
	pending           string
	place             string
	promote           string
	pruneArchive      string
	pruneBatches      string
	pruneLimits       string
	pruneServers      string
	pruneStats        string
	pruneUniques      string
	queues            string
	recent            string
	reclaim           string
	recurring         string
	recurrings        string
	releaseUniques    string
	removeRecurring   string
	requeueArchived   string
	requeueLive       string
	resign            string
	resolve           string
	seal              string
	series            string
	servers           string
	setMeta           string
	skipLimits        string
	stranded          string
	strandedStates    string
	throttledKeys     string
	touch             string
	unregister        string
	updateLive        string
	updateRecurring   string
	waiting           string
}

func newStatements(prefix string) statements {
	r := strings.NewReplacer("{p}", prefix)
	return statements{
		account:           r.Replace(sqlAccount),
		active:            r.Replace(sqlActive),
		advance:           r.Replace(sqlAdvance),
		allocate:          r.Replace(sqlAllocate),
		archive:           r.Replace(sqlArchive),
		archivedParents:   r.Replace(sqlArchivedParents),
		attach:            r.Replace(sqlAttach),
		awaiting:          r.Replace(sqlAwaiting),
		batch:             r.Replace(sqlBatch),
		batches:           r.Replace(sqlBatches),
		busy:              r.Replace(sqlBusy),
		cancel:            r.Replace(sqlCancel),
		claim:             r.Replace(sqlClaim),
		claimUniques:      r.Replace(sqlClaimUniques),
		completeBatches:   r.Replace(sqlCompleteBatches),
		count:             r.Replace(sqlCount),
		counts:            r.Replace(sqlCounts),
		createRecurring:   r.Replace(sqlCreateRecurring),
		declare:           r.Replace(sqlDeclare),
		declared:          r.Replace(sqlDeclared),
		directives:        r.Replace(sqlDirectives),
		dropFailed:        r.Replace(sqlDropFailed),
		dropHolders:       r.Replace(sqlDropHolders),
		dueRecurring:      r.Replace(sqlDueRecurring),
		ensure:            r.Replace(sqlEnsure),
		expiredFailed:     r.Replace(sqlExpiredFailed),
		finishedBatchDeps: r.Replace(sqlFinishedBatchDeps),
		fire:              r.Replace(sqlFire),
		fired:             r.Replace(sqlFired),
		hasRecurring:      r.Replace(sqlHasRecurring),
		heartbeat:         r.Replace(sqlHeartbeat),
		holderPage:        r.Replace(sqlHolderPage),
		idleBatches:       r.Replace(sqlIdleBatches),
		insertDeps:        r.Replace(sqlInsertDeps),
		insertDoomed:      r.Replace(sqlInsertDoomed),
		insertJobs:        r.Replace(sqlInsertJobs),
		insertPlain:       r.Replace(sqlInsertPlain),
		job:               r.Replace(sqlJob),
		lead:              r.Replace(sqlLead),
		limitInfo:         r.Replace(sqlLimitInfo),
		limitPage:         r.Replace(sqlLimitPage),
		lockArchived:      r.Replace(sqlLockArchived),
		lockBatch:         r.Replace(sqlLockBatch),
		lockBatches:       r.Replace(sqlLockBatches),
		lockChildren:      r.Replace(sqlLockChildren),
		lockFailed:        r.Replace(sqlLockFailed),
		lockLimits:        r.Replace(sqlLockLimits),
		lockParents:       r.Replace(sqlLockParents),
		lockRunning:       r.Replace(sqlLockRunning),
		lockTargets:       r.Replace(sqlLockTargets),
		nextDue:           r.Replace(sqlNextDue),
		openBatch:         r.Replace(sqlOpenBatch),
		orphans:           r.Replace(sqlOrphans),
		pause:             r.Replace(sqlPause),
		pending:           r.Replace(sqlPending),
		place:             r.Replace(sqlPlace),
		promote:           r.Replace(sqlPromote),
		pruneArchive:      r.Replace(sqlPruneArchive),
		pruneBatches:      r.Replace(sqlPruneBatches),
		pruneLimits:       r.Replace(sqlPruneLimits),
		pruneServers:      r.Replace(sqlPruneServers),
		pruneStats:        r.Replace(sqlPruneStats),
		pruneUniques:      r.Replace(sqlPruneUniques),
		queues:            r.Replace(sqlQueues),
		recent:            r.Replace(sqlRecent),
		reclaim:           r.Replace(sqlReclaim),
		recurring:         r.Replace(sqlRecurring),
		recurrings:        r.Replace(sqlRecurrings),
		releaseUniques:    r.Replace(sqlReleaseUniques),
		removeRecurring:   r.Replace(sqlRemoveRecurring),
		requeueArchived:   r.Replace(sqlRequeueArchived),
		requeueLive:       r.Replace(sqlRequeueLive),
		resign:            r.Replace(sqlResign),
		resolve:           r.Replace(sqlResolve),
		seal:              r.Replace(sqlSeal),
		series:            r.Replace(sqlSeries),
		servers:           r.Replace(sqlServers),
		setMeta:           r.Replace(sqlSetMeta),
		skipLimits:        r.Replace(sqlSkipLimits),
		stranded:          r.Replace(sqlStranded),
		strandedStates:    r.Replace(sqlStrandedStates),
		throttledKeys:     r.Replace(sqlThrottledKeys),
		touch:             r.Replace(sqlTouch),
		unregister:        r.Replace(sqlUnregister),
		updateLive:        r.Replace(sqlUpdateLive),
		updateRecurring:   r.Replace(sqlUpdateRecurring),
		waiting:           r.Replace(sqlWaiting),
	}
}
