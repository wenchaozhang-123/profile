/*-------------------------------------------------------------------------
 *
 * pg_directory_table.h
 *	  definition of the "directory table" system catalog (pg_directory_table)
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_directory_table.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DIRECTORY_TABLE_H
#define PG_DIRECTORY_TABLE_H

#include "catalog/genbki.h"
#include "catalog/pg_directory_table_d.h"
#include "nodes/parsenodes.h"

/* ----------------
 *		pg_directory_table definition.  cpp turns this into
 *		typedef struct FormData_pg_directory_table
 * ----------------
 */
CATALOG(pg_directory_table,8545,DirectoryTableRelationId)
{
	/* OID of directory table */
	Oid			dtrelid BKI_LOOKUP(pg_class);

	/* identifier of table space for relation (0 means default for database) */
	Oid			dttablespace BKI_DEFAULT(0) BKI_LOOKUP_OPT(pg_tablespace);
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		dtlocation;		/* directory table location */
#endif
} FormData_pg_directory_table;

/* ----------------
 *		Form_pg_directory_table corresponds to a pointer to a tuple with
 *		the format of pg_directory_table relation.
 * ----------------
 */
typedef FormData_pg_directory_table *Form_pg_directory_table;

DECLARE_TOAST(pg_directory_table, 8546, 8547);

DECLARE_UNIQUE_INDEX_PKEY(pg_directory_table_relid_index, 8548, on pg_directory_table using btree(dtrelid oid_ops));
#define DirectoryTableRelidIndexId	8548

typedef struct DirectoryTable
{
	Oid			relId;		/* relation Oid */
	Oid			spcId;		/* tablespace Oid */
	char 		*location;	/* location */
} DirectoryTable;

extern DirectoryTable *GetDirectoryTable(Oid relId);
extern bool RelationIsDirectoryTable(Oid relId);
extern List *GetDirectoryTableBuiltinColumns(void);
extern void RemoveDirectoryTableEntry(Oid relId);

#endif /* PG_DIRECTORY_TABLE_H */
