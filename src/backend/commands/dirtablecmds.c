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
#include "storage/ufs.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static char *
trimLocation(char *value, char c)
{
	char *end;

	while (*value == c)
		value++;

	end = value + strlen(value) - 1;
	while (end > value && *end == c)
		end--;

	*(end + 1) = '\0';

	return value;
}

static Oid
chooseTablespace(CreateDirectoryTableStmt *stmt)
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
	Relation	dirRelation;
	Datum		values[Natts_pg_directory_table];
	bool		nulls[Natts_pg_directory_table];
	HeapTuple	tuple;
	char	   *newPath = trimLocation(stmt->location, '/');

	if (strlen(newPath) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid \"location\": only relative path can be used")));

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

	values[Anum_pg_directory_table_dtrelid - 1] = ObjectIdGetDatum(relId);
	values[Anum_pg_directory_table_dttablespace - 1] = chooseTablespace(stmt);
	values[Anum_pg_directory_table_dtlocation - 1] = CStringGetTextDatum(newPath);

	tuple = heap_form_tuple(dirRelation->rd_att, values, nulls);

	CatalogTupleInsert(dirRelation, tuple);

	heap_freetuple(tuple);

	table_close(dirRelation, RowExclusiveLock);
}

Datum
file_content(PG_FUNCTION_ARGS)
{
	text *arg0 = PG_GETARG_TEXT_PP(0);
	char *scopedUrl = text_to_cstring(arg0);
	char *tablespaceName;
	char *filePath;
	char  errorMessage[256];
	char  buffer[4096];
	char *data;
	int   curPos = 0;
	int   bytesRead;
	Oid   spcId;
	bytea   *result;
	UfsFile *file;
	int64    fileSize;

	/* /dir_tablespace/dir_table/animal/tab_a.bin */
	filePath = strchr(scopedUrl + 1, '/');
	tablespaceName = pnstrdup(scopedUrl + 1, filePath - scopedUrl - 1);
	spcId = get_tablespace_oid(tablespaceName, false);

	file = UfsFileOpen(spcId,
					   filePath,
					   O_RDONLY,
					   errorMessage,
					   sizeof(errorMessage));
	if (file == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to open file \"%s\": %s", filePath, errorMessage)));

	fileSize = UfsFileSize(file);
	result = (bytea *) palloc(fileSize + VARHDRSZ);
	SET_VARSIZE(result, fileSize + VARHDRSZ);
	data = VARDATA(result);

	while (true)
	{
		bytesRead = UfsFileRead(file, buffer, sizeof(buffer));
		if (bytesRead == -1)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to read file \"%s\": %s", filePath, UfsGetLastError(file))));

		if (bytesRead == 0)
			break;

		memcpy(data + curPos, buffer, bytesRead);
		curPos += bytesRead;
	}

	UfsFileClose(file);

	PG_RETURN_BYTEA_P(result);
}
