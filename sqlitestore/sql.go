package sqlitestore

import (
	"context"
	"database/sql"
	"strings"
)

const clock = `CAST(round((julianday('now') - 2440587.5) * 86400000) AS INTEGER) * 1000`

type statements struct {
	active            string
	activate          string
	admit             string
	advance           string
	afterBatches      string
	allocate          string
	archive           string
	archiveSucceeded  string
	archived          string
	attach            string
	awaiting          string
	batch             string
	batches           string
	cancel            string
	children          string
	claim             string
	claimKinds        string
	complete          string
	count             string
	counts            string
	createRecurring   string
	declare           string
	directives        string
	dropArchived      string
	dropDeps          string
	dropHolder        string
	dueRecurring      string
	expiredStats      string
	failed            string
	finishedBatchDeps string
	fire              string
	fired             string
	hasRecurring      string
	heartbeat         string
	hold              string
	holderPage        string
	holder            string
	idleBatches       string
	insertDeps        string
	insertDoomed      string
	insertJobs        string
	job               string
	lead              string
	limitPage         string
	lockBatch         string
	nextDue           string
	now               string
	openBatch         string
	openBatches       string
	orphans           string
	parents           string
	pause             string
	promote           string
	pruneArchive      string
	pruneBatches      string
	pruneFailed       string
	pruneLimits       string
	pruneServers      string
	pruneUniques      string
	queues            string
	recurring         string
	recurrings        string
	releaseBatches    string
	release           string
	remove            string
	removeRecurring   string
	requeueArchived   string
	requeueLive       string
	resign            string
	resolve           string
	restore           string
	running           string
	seal              string
	series            string
	servers           string
	setMeta           string
	slots             string
	stranded          string
	strandedStates    string
	targets           string
	throttledKeys     string
	unregister        string
	update            string
	updateRecurring   string
}

func render(prefix string) statements {
	r := strings.NewReplacer("{p}", prefix, "{now}", clock).Replace
	return statements{
		active:            r(sqlActive),
		activate:          r(sqlActivate),
		admit:             r(sqlAdmit),
		advance:           r(sqlAdvance),
		afterBatches:      r(sqlAfterBatches),
		allocate:          r(sqlAllocate),
		archive:           r(sqlArchive),
		archiveSucceeded:  r(sqlArchiveSucceeded),
		archived:          r(sqlArchived),
		attach:            r(sqlAttach),
		awaiting:          r(sqlAwaiting),
		batch:             r(sqlBatch),
		batches:           r(sqlBatches),
		cancel:            r(sqlCancel),
		children:          r(sqlChildren),
		claim:             r(sqlClaim),
		claimKinds:        r(sqlClaimKinds),
		complete:          r(sqlComplete),
		count:             r(sqlCount),
		counts:            r(sqlCounts),
		createRecurring:   r(sqlCreateRecurring),
		declare:           r(sqlDeclare),
		directives:        r(sqlDirectives),
		dropArchived:      r(sqlDropArchived),
		dropDeps:          r(sqlDropDeps),
		dropHolder:        r(sqlDropHolder),
		dueRecurring:      r(sqlDueRecurring),
		expiredStats:      r(sqlExpiredStats),
		failed:            r(sqlFailed),
		finishedBatchDeps: r(sqlFinishedBatchDeps),
		fire:              r(sqlFire),
		fired:             r(sqlFired),
		hasRecurring:      r(sqlHasRecurring),
		heartbeat:         r(sqlHeartbeat),
		hold:              r(sqlHold),
		holderPage:        r(sqlHolderPage),
		holder:            r(sqlHolder),
		idleBatches:       r(sqlIdleBatches),
		insertDeps:        r(sqlInsertDeps),
		insertDoomed:      r(sqlInsertDoomed),
		insertJobs:        r(sqlInsertJobs),
		job:               r(sqlJob),
		lead:              r(sqlLead),
		limitPage:         r(sqlLimitPage),
		lockBatch:         r(sqlLockBatch),
		nextDue:           r(sqlNextDue),
		now:               r(sqlNow),
		openBatch:         r(sqlOpenBatch),
		openBatches:       r(sqlOpenBatches),
		orphans:           r(sqlOrphans),
		parents:           r(sqlParents),
		pause:             r(sqlPause),
		promote:           r(sqlPromote),
		pruneArchive:      r(sqlPruneArchive),
		pruneBatches:      r(sqlPruneBatches),
		pruneFailed:       r(sqlPruneFailed),
		pruneLimits:       r(sqlPruneLimits),
		pruneServers:      r(sqlPruneServers),
		pruneUniques:      r(sqlPruneUniques),
		queues:            r(sqlQueues),
		recurring:         r(sqlRecurring),
		recurrings:        r(sqlRecurrings),
		releaseBatches:    r(sqlReleaseBatches),
		release:           r(sqlRelease),
		remove:            r(sqlRemove),
		removeRecurring:   r(sqlRemoveRecurring),
		requeueArchived:   r(sqlRequeueArchived),
		requeueLive:       r(sqlRequeueLive),
		resign:            r(sqlResign),
		resolve:           r(sqlResolve),
		restore:           r(sqlRestore),
		running:           r(sqlRunning),
		seal:              r(sqlSeal),
		series:            r(sqlSeries),
		servers:           r(sqlServers),
		setMeta:           r(sqlSetMeta),
		slots:             r(sqlSlots),
		stranded:          r(sqlStranded),
		strandedStates:    r(sqlStrandedStates),
		targets:           r(sqlTargets),
		throttledKeys:     r(sqlThrottledKeys),
		unregister:        r(sqlUnregister),
		update:            r(sqlUpdate),
		updateRecurring:   r(sqlUpdateRecurring),
	}
}

func each(rows *sql.Rows, err error, fn func() error) error {
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := fn(); err != nil {
			return err
		}
	}
	return rows.Err()
}

func execEach(ctx context.Context, q querier, query string, width int, rows []any) error {
	if len(rows) == 0 {
		return nil
	}
	st, done, err := prepare(ctx, q, query)
	if err != nil {
		return err
	}
	defer done()
	for i := 0; i < len(rows); i += width {
		if _, err := st.ExecContext(ctx, rows[i:i+width]...); err != nil {
			return err
		}
	}
	return nil
}

const insertVars = 80

func insertRows(ctx context.Context, q querier, head, tail string, width int, rows []any) error {
	per := max(insertVars/width, 1) * width
	full := len(rows) - len(rows)%per
	if full > 0 {
		if err := execEach(ctx, q, head+tuples(per/width, width)+tail, per, rows[:full]); err != nil {
			return err
		}
	}
	if rest := rows[full:]; len(rest) > 0 {
		return execEach(ctx, q, head+tuples(len(rest)/width, width)+tail, len(rest), rest)
	}
	return nil
}

func tuples(n, width int) string {
	row := "(" + strings.Repeat("?, ", width-1) + "?)"
	return strings.Repeat(row+", ", n-1) + row
}

func optInt(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}

func optString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
