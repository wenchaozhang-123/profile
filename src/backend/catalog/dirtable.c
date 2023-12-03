/*-------------------------------------------------------------------------
 *
 * dirtable.c
 *		  support for directory table.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/backend/catalog/dirtable.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/pg_directory_table.h"
#include "cdb/cdbutil.h"
#include "commands/defrem.h"
#include "catalog/dirtable.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

typedef struct DirTableColumnDesc
{
	const char *colName;
	const char *typName;
} DirTableColumnDesc;

static const DirTableColumnDesc dirTableColumns[] = {
	{"scoped_file_url", "text"},
	{"relative_path", "text"},
	{"size", "int8"},
	{"last_modified", "timestamptz"}
};

/*
 * GetDirectoryTable - look up the directory table definition by relation oid.
 */
DirectoryTable *
GetDirectoryTable(Oid relId)
{
	Form_pg_directory_table tableForm;
	HeapTuple	tuple;
	Datum		datum;
	bool		isNull;
	DirectoryTable  *dirTable;

	tuple = SearchSysCache1(DIRECTORYTABLEREL, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for directory table %u", relId);

	tableForm = (Form_pg_directory_table) GETSTRUCT(tuple);

	dirTable = (DirectoryTable *) palloc(sizeof(DirectoryTable));
	dirTable->relId = relId;
	dirTable->spcId = tableForm->dttablespace;

	datum = SysCacheGetAttr(DIRECTORYTABLEREL,
							tuple,
							Anum_pg_directory_table_dtlocation,
							&isNull);
	Assert(!isNull);

	dirTable->location = TextDatumGetCString(datum);
	ReleaseSysCache(tuple);

	return dirTable;
}

bool
RelationIsDirectoryTable(Oid relId)
{
	HeapTuple tuple;

	tuple = SearchSysCache1(DIRECTORYTABLEREL, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))
		return false;

	ReleaseSysCache(tuple);
	return true;
}

List *
GetDirectoryTableBuiltinColumns(void)
{
	int			 i;
	List		*result = NIL;

	for (i = 0; i < lengthof(dirTableColumns); i++)
	{
		ColumnDef *columnDef = makeNode(ColumnDef);

		columnDef->colname = pstrdup(dirTableColumns[i].colName);
		columnDef->typeName = SystemTypeName(pstrdup(dirTableColumns[i].typName));
		columnDef->is_local = true;

		result = lappend(result, columnDef);
	}

	return result;
}

void
RemoveDirectoryTableEntry(Oid relId)
{
	Relation	rel;
	HeapTuple	tuple;

	rel = table_open(DirectoryTableRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(DIRECTORYTABLEREL, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for directory table %u", relId);

	CatalogTupleDelete(rel, &tuple->t_self);

	ReleaseSysCache(tuple);
	table_close(rel, RowExclusiveLock);
}
