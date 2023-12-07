/*-------------------------------------------------------------------------
 *
 * directorycmds.c
 *	  directory table creation/manipulation commands
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/directorycmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_directory_table.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdboidsync.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "commands/dirtablecmds.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#define DIRECTORY_TABLE_FUNCTION_COLUMNS	7

typedef struct TableFunctionContext
{
	Relation 		relation;
	TableScanDesc	scanDesc;
	TupleTableSlot	*slot;
	DirectoryTable 	*dirTable;
} TableFunctionContext;

Datum directory_table(PG_FUNCTION_ARGS);

static char *
getDirectoryTablePath(Oid spcId, Oid dbId, RelFileNodeId relId)
{
	return psprintf("%u/%u/dirtable/"UINT64_FORMAT, spcId, dbId, relId);
}

static Oid
chooseTableSpace(CreateDirectoryTableStmt *stmt)
{
	Oid tablespaceId = InvalidOid;

	/*
	 * Select tablespace to use: an explicitly indicated one, or (in the case
	 * of a partitioned table) the parent's, if it has one.
	 */
	if (stmt->tablespacename)
	{
		/*
		 * Tablespace specified on the command line, or was passed down by
		 * dispatch.
		 */
		tablespaceId = get_tablespace_oid(stmt->tablespacename, false);
	}

	/* still nothing? use the default */
	if (!OidIsValid(tablespaceId))
		tablespaceId = GetDefaultTablespace(stmt->base.relation->relpersistence, false);

	/* Check permissions except when using database's default */
	if (OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace)
	{
		AclResult	aclresult;

		aclresult = pg_tablespace_aclcheck(tablespaceId, GetUserId(),
									 		ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_TABLESPACE,
				  			get_tablespace_name(tablespaceId));
	}

	/* In all cases disallow placing user relations in pg_global */
	if (tablespaceId == GLOBALTABLESPACE_OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("only shared relations can be placed in pg_global tablespace")));

	return tablespaceId;
}

void
CreateDirectoryTable(CreateDirectoryTableStmt *stmt, Oid relId)
{
	Relation 	dirRelation;
	Datum 		values[Natts_pg_directory_table];
	bool 		nulls[Natts_pg_directory_table];
	HeapTuple	tuple;
	char 		*dirTablePath;
	Oid 		spcId = chooseTableSpace(stmt);

	dirTablePath = getDirectoryTablePath(spcId, MyDatabaseId, stmt->relnode);

	/*
	 * Advance command counter to ensure the pg_attribute tuple is visible;
	 * the tuple might be updated to add constraints in previous step.
	 */
	CommandCounterIncrement();

	dirRelation = table_open(DirectoryTableRelationId, RowExclusiveLock);
	/*
	 * Insert tuple into pg_directory_table.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_directory_table_dttablespace - 1] = spcId;
	values[Anum_pg_directory_table_dtrelid - 1] = ObjectIdGetDatum(relId);
	values[Anum_pg_directory_table_dtlocation - 1] = CStringGetTextDatum(dirTablePath);

	tuple = heap_form_tuple(dirRelation->rd_att, values, nulls);

	CatalogTupleInsert(dirRelation, tuple);

	heap_freetuple(tuple);

	table_close(dirRelation, RowExclusiveLock);
}