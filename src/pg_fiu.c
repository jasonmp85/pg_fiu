#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#ifdef HAVE_DLOPEN
#include <dlfcn.h>
#else
#error DLOPEN is required
#endif
#include "/Users/jason/Documents/Code/libfiu/libfiu/fiu-control.h"

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "nodes/pg_list.h"
#include "parser/analyze.h"
#include "server/fmgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

#define ANY_STACK_POS -1

/* #include <fiu-control.h> */

/*
 * You can include more files here if needed.
 * To use some types, you must include the
 * correct file here based on:
 * http://www.postgresql.org/docs/current/static/xfunc-c.html#XFUNC-C-TYPE-TABLE
 */

PG_MODULE_MAGIC;

void _PG_init(void);

static bool InjectNextTransaction = false;
static post_parse_analyze_hook_type PrevPostParseAnalyzeHook = NULL;
static ExecutorStart_hook_type PrevExecutorStartHook = NULL;
static ProcessUtility_hook_type PrevProcessUtilityHook = NULL;

static bool FailurePointsInstalled = false;
static List *FailurePointList = NIL;


/* Different methods to decide when a point of failure fails */
/* TODO: Add FAIL_SQL to make using PostgreSQL calling conventions easier */
typedef enum
{
	FAIL_INVALID = 0,
	FAIL_ALWAYS,
	FAIL_SOMETIMES,
	FAIL_EXTERNAL,
	FAIL_STACK
} FailureMethod;

typedef struct FailurePoint
{
	char *name;
	int failRetval;
	void *failinfoRetval;
	bool failOnce;
	FailureMethod method;
	union
	{
		/* To use when method == FAIL_SOMETIMES */
		float likelihood;

		/* To use when method == FAIL_EXTERNAL */
		char *failurePredicateName;

		/* To use when method == FAIL_STACK */
		char *functionName;
	} methodDetails;
} FailurePoint;

static void PgFiuPostParseAnalyzeHook(ParseState *pstate, Query *query);
static void PgFiuExecutorStartHook(QueryDesc *queryDesc, int eflags);
static void PgFiuProcessUtilityHook(PlannedStmt *pstmt, const char *queryString,
									ProcessUtilityContext context, ParamListInfo params,
									QueryEnvironment *queryEnv, DestReceiver *dest,
									char *completionTag);
static void PgFiuXactCallback(XactEvent event, void *arg);
static void * DatumToCPointer(Datum datum, Oid type);
static void InstallFailurePoints();

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(add_failure_point);


/*
 * master_create_worker_shards is a user facing function to create worker shards
 * for the given relation in round robin order.
 */
Datum
add_failure_point(PG_FUNCTION_ARGS)
{
	text *failurePointText = PG_GETARG_TEXT_P(0);
	int32 failNum = PG_GETARG_INT32(1);
	Datum failinfoValue = PG_GETARG_DATUM(2);
	Oid failinfoType = get_fn_expr_argtype(fcinfo->flinfo, 2);
	void *failinfoPointer = DatumToCPointer(failinfoValue, failinfoType);
	bool failOnce = PG_GETARG_BOOL(3);
	MemoryContext oldContext;
	FailurePoint *fp;

	if (FailurePointsInstalled)
	{
		ereport(ERROR, (errmsg("cannot add failure points while points installed")));
	}

	oldContext = MemoryContextSwitchTo(TopMemoryContext);

	fp = palloc0(sizeof(FailurePoint));
	fp->name = text_to_cstring(failurePointText);
	fp->failRetval = failNum;
	fp->failinfoRetval = failinfoPointer;
	fp->failOnce = failOnce;

	FailurePointList = lappend(FailurePointList, fp);
	MemoryContextSwitchTo(oldContext);

	PG_RETURN_BOOL(true);
}


static void *
DatumToCPointer(Datum datum, Oid type)
{
	void *failinfoPointer = NULL;

	switch (type)
	{
		case BOOLOID:
		{
			bool *value = MemoryContextAllocZero(TopMemoryContext, sizeof(bool));
			*value = DatumGetBool(datum);

			failinfoPointer = (void *) value;
			break;
		}

		case INT2OID:
		{
			int16 *value = MemoryContextAllocZero(TopMemoryContext, sizeof(int16));
			*value = DatumGetInt16(datum);

			failinfoPointer = (void *) value;
			break;
		}

		case INT4OID:
		{
			int32 *value = MemoryContextAllocZero(TopMemoryContext, sizeof(int32));
			*value = DatumGetInt32(datum);

			failinfoPointer = (void *) value;
			break;
		}

		case INT8OID:
		{
			int64 *value = MemoryContextAllocZero(TopMemoryContext, sizeof(int64));
			*value = DatumGetInt64(datum);

			failinfoPointer = (void *) value;
			break;
		}

		case FLOAT4OID:
		{
			float4 *value = MemoryContextAllocZero(TopMemoryContext, sizeof(float4));
			*value = DatumGetFloat4(datum);

			failinfoPointer = (void *) value;
			break;
		}

		case FLOAT8OID:
		{
			float8 *value = MemoryContextAllocZero(TopMemoryContext, sizeof(float8));
			*value = DatumGetFloat8(datum);

			failinfoPointer = (void *) value;
			break;
		}

		case NUMERICOID:
		{
			float8 *value = MemoryContextAllocZero(TopMemoryContext, sizeof(float8));
			*value = DatumGetFloat8(DirectFunctionCall1(numeric_float8, datum));

			failinfoPointer = (void *) value;
			break;
		}

		case CHAROID:
		{
			char *value = MemoryContextAllocZero(TopMemoryContext, sizeof(value));
			*value = DatumGetChar(datum);

			failinfoPointer = (void *) value;
			break;
		}

		case TEXTOID:
		{
			char *value = TextDatumGetCString(datum);
			value = MemoryContextStrdup(TopMemoryContext, value);

			failinfoPointer = (void *) value;
			break;
		}

		default:
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("failinfo of type %s is not supported",
								   format_type_be(type))));
			break;
		}
	}

	return failinfoPointer;
}


void
_PG_init(void)
{
	const int GucFlags = (GUC_DISALLOW_IN_FILE |
						  GUC_SUPERUSER_ONLY |
						  GUC_DISALLOW_IN_AUTO_FILE);

	StaticAssertStmt(sizeof(int) >= 4, "Unsupported platform: int too small");

	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the pg_stat_statements functions to be created even when the
	 * module isn't active.  The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
	{
		return;
	}

	/*
	 * Define (or redefine) custom GUC variables.
	 */
	DefineCustomBoolVariable("pg_fiu.inject_next_xact",
							 "Whether fault injection will be performed "
							 "during the next transaction.",
							 NULL, &InjectNextTransaction, false,
							 GucFlags, 0, NULL, NULL, NULL);

	PrevPostParseAnalyzeHook = post_parse_analyze_hook;
	post_parse_analyze_hook = PgFiuPostParseAnalyzeHook;
	PrevExecutorStartHook = ExecutorStart_hook;
	ExecutorStart_hook = PgFiuExecutorStartHook;
	PrevProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = PgFiuProcessUtilityHook;

	RegisterXactCallback(PgFiuXactCallback, NULL);
}


static void
PgFiuPostParseAnalyzeHook(ParseState *pstate, Query *query)
{
	InstallFailurePoints();

	if (PrevPostParseAnalyzeHook != NULL)
	{
		PrevPostParseAnalyzeHook(pstate, query);
	}
}


static void
PgFiuExecutorStartHook(QueryDesc *queryDesc, int eflags)
{
	InstallFailurePoints();

	if (PrevExecutorStartHook != NULL)
	{
		PrevExecutorStartHook(queryDesc, eflags);
	}
}


static void
PgFiuProcessUtilityHook(PlannedStmt *pstmt, const char *queryString,
						ProcessUtilityContext context, ParamListInfo params,
						QueryEnvironment *queryEnv, DestReceiver *dest,
						char *completionTag)
{
	bool isInjectionDisableCommand = false;

	if (!isInjectionDisableCommand)
	{
		InstallFailurePoints();
	}

	if (PrevProcessUtilityHook != NULL)
	{
		PrevProcessUtilityHook(pstmt, queryString, context, params,
							   queryEnv, dest, completionTag);
	}
}


static void
PgFiuXactCallback(XactEvent event, void *arg)
{
	InstallFailurePoints();
}


static void
InstallFailurePoints()
{
	ListCell *failurePointCell;

	if (FailurePointsInstalled)
	{
		return;
	}

	foreach(failurePointCell, FailurePointList)
	{
		FailurePoint *fp = lfirst(failurePointCell);
		const char *name = fp->name;
		int failnum = fp->failRetval;
		void *failinfo = fp->failinfoRetval;
		unsigned int flags = fp->failOnce ? FIU_ONETIME : 0;

		switch (fp->method)
		{
			case (FAIL_ALWAYS):
			{
				fiu_enable(name, failnum, failinfo, flags);
				break;
			}

			case (FAIL_SOMETIMES):
			{
				fiu_enable_random(name, failnum, failinfo, flags,
								  fp->methodDetails.likelihood);
				break;
			}

			case (FAIL_EXTERNAL):
			{
				external_cb_t *callback = (external_cb_t *) dlsym(RTLD_SELF,
																  fp->methodDetails.
																  functionName);
				fiu_enable_external(name, failnum, failinfo, flags, callback);
				break;
			}

			case (FAIL_STACK):
			{
				fiu_enable_stack_by_name(name, failnum, failinfo, flags,
										 fp->methodDetails.functionName, ANY_STACK_POS);
				break;
			}

			default:
			{
				ereport(ERROR, (errmsg("unexpected error")));
			}
		}
	}
}
