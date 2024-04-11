/*-------------------------------------------------------------------------
 *
 * pg_directory_table.c
 *		  support for directory table.
 *
 * Copyright (c) 2016-Present Hashdata, Inc.
 *
 * IDENTIFICATION
 *		  src/backend/catalog/pg_directory_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "parser/parser.h"
#include "parser/parse_func.h"
#include "catalog/pg_directory_table.h"
#include "catalog/pg_tablespace.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

typedef FileAm* (*File_handler) (void);
/* Hash table for tablespace file handler */
static HTAB *TableSpaceFileHandlerHash = NULL;

typedef struct TableSpaceFileAmEntry
{
	Oid 	spcId;	/* tablespace oid */
	FileAm  *spcAm;	/* tablespace file am */
} TableSpaceFileAmEntry;

typedef struct DirTableColumnDesc
{
	const char *colName;
	const char *typName;
} DirTableColumnDesc;

static const DirTableColumnDesc dirTableColumns[] = {
	{"relative_path", "text"},
	{"size", "int8"},
	{"last_modified", "timestamptz"},
	{"md5", "text"},
	{"tag", "text"}
};

static void
invalidateTableSpaceFileAmCallBack(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS	status;
	TableSpaceFileAmEntry	*fileAmEntry;

	hash_seq_init(&status, TableSpaceFileHandlerHash);
	while ((fileAmEntry = (TableSpaceFileAmEntry *) hash_seq_search(&status)) != NULL)
	{
		hash_search(TableSpaceFileHandlerHash,
					(void *) &fileAmEntry->spcId,
					HASH_REMOVE,
					NULL);
	}
}

static void
InitializeTableSpaceFileHandlerHash(void)
{
	HASHCTL		ctl;

	/* Initialize the hash table. */
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(TableSpaceFileAmEntry);
	TableSpaceFileHandlerHash =
		hash_create("TableSpace File Handler hash", 8, &ctl,
			  		HASH_ELEM | HASH_BLOBS);

	/* Make sure we've initialized CacheMemoryContext. */
	if (!CacheMemoryContext)
		CreateCacheMemoryContext();

	/* Watch for invalidation events. */
	CacheRegisterSyscacheCallback(TABLESPACEOID,
								  invalidateTableSpaceFileAmCallBack,
								  (Datum) 0);
}

FileAm *
GetTablespaceFileHandler(Oid spcId)
{
	HeapTuple tuple;
	Datum datum;
	bool isNull;
	char *fileHandler;
	List	*fileHandler_list;
	Form_pg_tablespace tblspcForm;
	void	   *libraryhandle;
	char	   *prosrc;
	char	   *probin;
	File_handler file_handler;
	TableSpaceFileAmEntry *spcAmEntry;
	FileAm	*spcAm;
	bool 	found;

	if (!TableSpaceFileHandlerHash)
		InitializeTableSpaceFileHandlerHash();

	spcAmEntry = (TableSpaceFileAmEntry *) hash_search(TableSpaceFileHandlerHash,
													   (void *) &spcId,
													   HASH_FIND,
													   &found);

	if (found)
		return spcAmEntry->spcAm;

	tuple = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(spcId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for table space %u", spcId);

	tblspcForm = (Form_pg_tablespace) GETSTRUCT(tuple);
	datum = SysCacheGetAttr(TABLESPACEOID,
							tuple,
							Anum_pg_tablespace_spcfilehandler,
							&isNull);
	if (!isNull)
	{
		fileHandler = TextDatumGetCString(datum);

		if (!SplitIdentifierString(fileHandler, ',', &fileHandler_list))
			ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid list syntax for \"spcfilehandler\" option")));

		if (list_length(fileHandler_list) != 2)
			ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid syntax for \"handler\" option")));

		probin = (char *) linitial(fileHandler_list);
		prosrc = (char *) lsecond(fileHandler_list);

		file_handler = (File_handler) load_external_function(probin, prosrc, true, &libraryhandle);

		if (file_handler)
			spcAm = (*file_handler) ();

		if (spcAm == NULL || spcAm == &localFileAm)
			elog(ERROR, "tablespace file handler did not return a FileAm struct");
	}
	else
	{
		spcAm = &localFileAm;
	}

	ReleaseSysCache(tuple);

	Assert(spcAm != NULL);
	Assert(spcAm->open != NULL);
	Assert(spcAm->close != NULL);
	Assert(spcAm->read != NULL);
	Assert(spcAm->write != NULL);
	Assert(spcAm->size != NULL);
	Assert(spcAm->unlink != NULL);
	Assert(spcAm->formatFileName != NULL);
	Assert(spcAm->exists != NULL);
	Assert(spcAm->name != NULL);
	Assert(spcAm->getLastError != NULL);

	spcAmEntry = (TableSpaceFileAmEntry *) hash_search(TableSpaceFileHandlerHash,
													   (void *) &spcId,
													   HASH_ENTER,
													   &found);
	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("extra tablespace oid \"%u\" already exists", spcId)));

	spcAmEntry->spcAm = spcAm;

	return spcAm;
}

/*
 * GetDirectoryTable - look up the directory table definition by relId.
 */
DirectoryTable *
GetDirectoryTable(Oid relId)
{
	Form_pg_directory_table dirtableForm;
	HeapTuple tuple;
	Datum datum;
	bool isNull;
	DirectoryTable *dirTable;

	tuple = SearchSysCache1(DIRECTORYTABLEREL, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for directory table %u", relId);

	dirtableForm = (Form_pg_directory_table) GETSTRUCT(tuple);

	dirTable = (DirectoryTable *) palloc(sizeof(DirectoryTable));
	dirTable->relId = relId;
	dirTable->spcId = dirtableForm->dttablespace;
	GetTablespaceFileHandler(dirtableForm->dttablespace);

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
	int i;
	List *result = NIL;

	for (i = 0; i < lengthof(dirTableColumns); i++)
	{
		ColumnDef *columnDef = makeNode(ColumnDef);

		columnDef->colname = pstrdup(dirTableColumns[i].colName);
		columnDef->typeName = SystemTypeName(pstrdup(dirTableColumns[i].typName));
		columnDef->is_local = true;

		if (i == 0)
		{
			Constraint *constraint = makeNode(Constraint);
			constraint->contype = CONSTR_PRIMARY;
			constraint->location = -1;
			constraint->keys = NIL;
			constraint->options = NIL;
			constraint->indexname = NULL;
			constraint->indexspace = NULL;
			columnDef->constraints = list_make1(constraint);
		}

		result = lappend(result, columnDef);
	}

	return result;
}

void
RemoveDirectoryTableEntry(Oid relId)
{
	Relation	rel;
	HeapTuple 	tuple;

	rel = table_open(DirectoryTableRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(DIRECTORYTABLEREL, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for directory table %u", relId);

	CatalogTupleDelete(rel, &tuple->t_self);

	ReleaseSysCache(tuple);
	table_close(rel, RowExclusiveLock);
}
