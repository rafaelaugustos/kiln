package mysqlstore

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
	_ driver.Console     = (*Store)(nil)
	_ driver.TxWriter    = (*TxWriter)(nil)
)

// Store is a [driver.Store] on MySQL. It also implements [driver.Transactor], [driver.LimitReader]
// and [driver.Console], and [driver.Notifier] through the bus given with [Bus]. It is safe for
// concurrent use.
type Store struct {
	db     *sql.DB
	prefix string
	q      statements
	side   *side
	seq    *allocator
	budget int
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

// New returns a store on db, whose DSN must name the database. Unless [NoMigrate] is given, New
// first migrates the tables as [Migrate] does; with NoMigrate it only checks them and fails when a
// migration or change of this release is missing. Either way it fails when the tables' migration
// number is newer than this release knows. The store keeps one of db's connections for itself, so
// New fails with [driver.ErrInvalid] when db allows a single open connection.
func New(ctx context.Context, db *sql.DB, opts ...Option) (*Store, error) {
	c := newConfig(opts)
	if !validPrefix(c.prefix) {
		return nil, fmt.Errorf("%w: prefix %q", driver.ErrInvalid, c.prefix)
	}
	if db.Stats().MaxOpenConnections == 1 {
		return nil, fmt.Errorf("%w: mysqlstore needs at least two open connections", driver.ErrInvalid)
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
	var packet int
	if err := db.QueryRowContext(ctx, "SELECT @@max_allowed_packet").Scan(&packet); err != nil {
		return nil, fmt.Errorf("kiln: max_allowed_packet: %w", err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("kiln: connect: %w", err)
	}
	q := newStatements(c.prefix)
	sc := &side{db: db, conn: conn}
	s := &Store{
		db:     db,
		prefix: c.prefix,
		q:      q,
		side:   sc,
		seq:    &allocator{side: sc, stmt: q.allocate},
		budget: min(maxStatement, max(packet-4<<10, 64<<10)),
		bus:    c.bus,
	}
	if c.bus != nil {
		s.nt = newNotifier(c.bus)
	}
	return s, nil
}

// Close publishes the events still pending, when the store has a bus, and gives back the
// connection the store kept. It closes neither db nor the bus. Call it after nothing uses the
// store any more; calling it again does nothing.
func (s *Store) Close() {
	s.nt.close()
	s.side.close()
}

// Tx returns a writer that inserts jobs and opens and seals batches inside tx, so that they
// commit or roll back with the application's work. tx must be open on the store's database. Begin
// it with [sql.LevelReadCommitted]: at REPEATABLE READ, InnoDB takes gap locks that can make
// concurrent enqueues wait for its commit. A nil tx gives a writer whose writes fail with
// [driver.ErrNilTx].
func (s *Store) Tx(tx *sql.Tx) *TxWriter {
	return &TxWriter{s: s, tx: tx}
}

type statements struct {
	active              string
	advance             string
	advanceTail         string
	afterBatches        string
	allocate            string
	appendLogs          string
	appendLogsTail      string
	archive             string
	archivedParents     string
	archiveTail         string
	awaiting            string
	batch               string
	batches             string
	busy                string
	cancel              string
	claim               string
	claimKinds          string
	claimUniques        string
	claimUniquesTail    string
	completeBatches     string
	count               string
	counts              string
	declare             string
	declareTail         string
	declared            string
	ensureTail          string
	createRecurring     string
	directives          string
	dropArchived        string
	dropDeps            string
	dropLogs            string
	due                 string
	doneBatches         string
	dueRecurring        string
	enqueue             string
	expiredArchive      string
	expiredFailed       string
	expiredStats        string
	expiredUniques      string
	finishBatches       string
	finishedBatchDeps   string
	fire                string
	fired               string
	hasRecurring        string
	heartbeat           string
	held                string
	dropHolders         string
	dropStats           string
	holderPage          string
	holders             string
	idleBatches         string
	insertDeps          string
	insertDoomed        string
	insertJobs          string
	job                 string
	keyHolders          string
	keyState            string
	lead                string
	leader              string
	limitInfo           string
	limitPage           string
	linkedNow           string
	lockArchived        string
	lockBatch           string
	lockBatches         string
	lockChildren        string
	lockFailed          string
	lockHolders         string
	lockLimits          string
	lockParentBatches   string
	lockParents         string
	lockRunning         string
	lockTargets         string
	logs                string
	nestedBatches       string
	nextDue             string
	openBatch           string
	openBatchDeps       string
	openDeps            string
	orphans             string
	pause               string
	pending             string
	promote             string
	pruneBatches        string
	pruneLimits         string
	pruneServers        string
	pruneUniques        string
	queues              string
	recent              string
	reclaimTail         string
	recurring           string
	recurrings          string
	releaseUniques      string
	remove              string
	removeRecurring     string
	replace             string
	replaceTail         string
	requeueArchived     string
	requeueArchivedTail string
	requeueLive         string
	requeueLiveTail     string
	reserveTail         string
	resolved            string
	resign              string
	seal                string
	series              string
	servers             string
	setMeta             string
	setProgress         string
	stranded            string
	take                string
	throttledKeys       string
	touch               string
	touchTail           string
	unregister          string
	unusedLimits        string
	updateLive          string
	updateLiveTail      string
	updateRecurring     string
	waiting             string
}

func newStatements(prefix string) statements {
	r := strings.NewReplacer("{p}", prefix)
	return statements{
		active:              r.Replace(sqlActive),
		advance:             r.Replace(sqlAdvance),
		advanceTail:         r.Replace(sqlAdvanceTail),
		afterBatches:        r.Replace(sqlAfterBatches),
		allocate:            r.Replace(sqlAllocate),
		appendLogs:          r.Replace(sqlAppendLogs),
		appendLogsTail:      r.Replace(sqlAppendLogsTail),
		archive:             r.Replace(sqlArchive),
		archivedParents:     r.Replace(sqlArchivedParents),
		archiveTail:         r.Replace(sqlArchiveTail),
		awaiting:            r.Replace(sqlAwaiting),
		batch:               r.Replace(sqlBatch),
		batches:             r.Replace(sqlBatches),
		busy:                r.Replace(sqlBusy),
		cancel:              r.Replace(sqlCancel),
		claim:               r.Replace(sqlClaim),
		claimKinds:          r.Replace(sqlClaimKinds),
		claimUniques:        r.Replace(sqlClaimUniques),
		claimUniquesTail:    r.Replace(sqlClaimUniquesTail),
		completeBatches:     r.Replace(sqlCompleteBatches),
		count:               r.Replace(sqlCount),
		counts:              r.Replace(sqlCounts),
		declare:             r.Replace(sqlDeclare),
		declareTail:         r.Replace(sqlDeclareTail),
		declared:            r.Replace(sqlDeclared),
		ensureTail:          r.Replace(sqlEnsureTail),
		createRecurring:     r.Replace(sqlCreateRecurring),
		directives:          r.Replace(sqlDirectives),
		dropArchived:        r.Replace(sqlDropArchived),
		dropDeps:            r.Replace(sqlDropDeps),
		dropLogs:            r.Replace(sqlDropLogs),
		due:                 r.Replace(sqlDue),
		doneBatches:         r.Replace(sqlDoneBatches),
		dueRecurring:        r.Replace(sqlDueRecurring),
		enqueue:             r.Replace(sqlEnqueue),
		expiredArchive:      r.Replace(sqlExpiredArchive),
		expiredFailed:       r.Replace(sqlExpiredFailed),
		expiredStats:        r.Replace(sqlExpiredStats),
		expiredUniques:      r.Replace(sqlExpiredUniques),
		finishBatches:       r.Replace(sqlFinishBatches),
		finishedBatchDeps:   r.Replace(sqlFinishedBatchDeps),
		fire:                r.Replace(sqlFire),
		fired:               r.Replace(sqlFired),
		hasRecurring:        r.Replace(sqlHasRecurring),
		heartbeat:           r.Replace(sqlHeartbeat),
		held:                r.Replace(sqlHeld),
		dropHolders:         r.Replace(sqlDropHolders),
		dropStats:           r.Replace(sqlDropStats),
		holderPage:          r.Replace(sqlHolderPage),
		holders:             r.Replace(sqlHolders),
		idleBatches:         r.Replace(sqlIdleBatches),
		insertDeps:          r.Replace(sqlInsertDeps),
		insertDoomed:        r.Replace(sqlInsertDoomed),
		insertJobs:          r.Replace(sqlInsertJobs),
		job:                 r.Replace(sqlJob),
		keyHolders:          r.Replace(sqlKeyHolders),
		keyState:            r.Replace(sqlKeyState),
		lead:                r.Replace(sqlLead),
		leader:              r.Replace(sqlLeader),
		limitInfo:           r.Replace(sqlLimitInfo),
		limitPage:           r.Replace(sqlLimitPage),
		linkedNow:           r.Replace(sqlLinkedNow),
		lockArchived:        r.Replace(sqlLockArchived),
		lockBatch:           r.Replace(sqlLockBatch),
		lockBatches:         r.Replace(sqlLockBatches),
		lockChildren:        r.Replace(sqlLockChildren),
		lockFailed:          r.Replace(sqlLockFailed),
		lockHolders:         r.Replace(sqlLockHolders),
		lockLimits:          r.Replace(sqlLockLimits),
		lockParentBatches:   r.Replace(sqlLockParentBatches),
		lockParents:         r.Replace(sqlLockParents),
		lockRunning:         r.Replace(sqlLockRunning),
		lockTargets:         r.Replace(sqlLockTargets),
		logs:                r.Replace(sqlLogs),
		nestedBatches:       r.Replace(sqlNestedBatches),
		nextDue:             r.Replace(sqlNextDue),
		openBatch:           r.Replace(sqlOpenBatch),
		openBatchDeps:       r.Replace(sqlOpenBatchDeps),
		openDeps:            r.Replace(sqlOpenDeps),
		orphans:             r.Replace(sqlOrphans),
		pause:               r.Replace(sqlPause),
		pending:             r.Replace(sqlPending),
		promote:             r.Replace(sqlPromote),
		pruneBatches:        r.Replace(sqlPruneBatches),
		pruneLimits:         r.Replace(sqlPruneLimits),
		pruneServers:        r.Replace(sqlPruneServers),
		pruneUniques:        r.Replace(sqlPruneUniques),
		queues:              r.Replace(sqlQueues),
		recent:              r.Replace(sqlRecent),
		reclaimTail:         r.Replace(sqlReclaimTail),
		recurring:           r.Replace(sqlRecurring),
		recurrings:          r.Replace(sqlRecurrings),
		releaseUniques:      r.Replace(sqlReleaseUniques),
		remove:              r.Replace(sqlRemove),
		replace:             r.Replace(sqlReplace),
		replaceTail:         r.Replace(sqlReplaceTail),
		removeRecurring:     r.Replace(sqlRemoveRecurring),
		requeueArchived:     r.Replace(sqlRequeueArchived),
		requeueArchivedTail: r.Replace(sqlRequeueArchivedTail),
		requeueLive:         r.Replace(sqlRequeueLive),
		requeueLiveTail:     r.Replace(sqlRequeueLiveTail),
		reserveTail:         r.Replace(sqlReserveTail),
		resolved:            r.Replace(sqlResolved),
		resign:              r.Replace(sqlResign),
		seal:                r.Replace(sqlSeal),
		series:              r.Replace(sqlSeries),
		servers:             r.Replace(sqlServers),
		setMeta:             r.Replace(sqlSetMeta),
		setProgress:         r.Replace(sqlSetProgress),
		stranded:            r.Replace(sqlStranded),
		take:                r.Replace(sqlTake),
		throttledKeys:       r.Replace(sqlThrottledKeys),
		touch:               r.Replace(sqlTouch),
		touchTail:           r.Replace(sqlTouchTail),
		unregister:          r.Replace(sqlUnregister),
		unusedLimits:        r.Replace(sqlUnusedLimits),
		updateLive:          r.Replace(sqlUpdateLive),
		updateLiveTail:      r.Replace(sqlUpdateLiveTail),
		updateRecurring:     r.Replace(sqlUpdateRecurring),
		waiting:             r.Replace(sqlWaiting),
	}
}
