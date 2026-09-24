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
	_ driver.Store      = (*Store)(nil)
	_ driver.Transactor = (*Store)(nil)
	_ driver.Writer     = (*TxWriter)(nil)
)

type Store struct {
	db     *sql.DB
	prefix string
	q      statements
	side   *side
	seq    *allocator
	budget int

	mu      sync.Mutex
	cursors struct {
		parent   int64
		awaiting int64
		limit    string
		holder   []byte
	}
}

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
	return &Store{
		db:     db,
		prefix: c.prefix,
		q:      q,
		side:   sc,
		seq:    &allocator{side: sc, stmt: q.allocate},
		budget: min(maxStatement, max(packet-4<<10, 64<<10)),
	}, nil
}

func (s *Store) Close() {
	s.side.close()
}

func (s *Store) Tx(tx *sql.Tx) *TxWriter {
	return &TxWriter{s: s, tx: tx}
}

type statements struct {
	active              string
	advance             string
	advanceTail         string
	afterBatches        string
	allocate            string
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
	limitPage           string
	linkedNow           string
	lockArchived        string
	lockBatch           string
	lockBatches         string
	lockChildren        string
	lockFailed          string
	lockLimits          string
	lockParents         string
	lockRunning         string
	lockTargets         string
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
	reclaimTail         string
	recurring           string
	recurrings          string
	releaseUniques      string
	remove              string
	removeRecurring     string
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
	stranded            string
	take                string
	throttledKeys       string
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
		limitPage:           r.Replace(sqlLimitPage),
		linkedNow:           r.Replace(sqlLinkedNow),
		lockArchived:        r.Replace(sqlLockArchived),
		lockBatch:           r.Replace(sqlLockBatch),
		lockBatches:         r.Replace(sqlLockBatches),
		lockChildren:        r.Replace(sqlLockChildren),
		lockFailed:          r.Replace(sqlLockFailed),
		lockLimits:          r.Replace(sqlLockLimits),
		lockParents:         r.Replace(sqlLockParents),
		lockRunning:         r.Replace(sqlLockRunning),
		lockTargets:         r.Replace(sqlLockTargets),
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
		reclaimTail:         r.Replace(sqlReclaimTail),
		recurring:           r.Replace(sqlRecurring),
		recurrings:          r.Replace(sqlRecurrings),
		releaseUniques:      r.Replace(sqlReleaseUniques),
		remove:              r.Replace(sqlRemove),
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
		stranded:            r.Replace(sqlStranded),
		take:                r.Replace(sqlTake),
		throttledKeys:       r.Replace(sqlThrottledKeys),
		unregister:          r.Replace(sqlUnregister),
		unusedLimits:        r.Replace(sqlUnusedLimits),
		updateLive:          r.Replace(sqlUpdateLive),
		updateLiveTail:      r.Replace(sqlUpdateLiveTail),
		updateRecurring:     r.Replace(sqlUpdateRecurring),
		waiting:             r.Replace(sqlWaiting),
	}
}
