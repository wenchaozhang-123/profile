/*-------------------------------------------------------------------------
 *
 * dirtable.h
 *	  support for directory table.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * src/include/catalog/dirtable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DIRTABLE_H
#define DIRTABLE_H

#include "nodes/parsenodes.h"

typedef struct DirectoryTable
{
	Oid			relId;			/* relation Oid */
	Oid			spcId;			/* tablespace Oid */
	char	   *location;		/* location */
} DirectoryTable;

extern DirectoryTable *GetDirectoryTable(Oid relId);
extern bool RelationIsDirectoryTable(Oid relId);
extern List *GetDirectoryTableBuiltinColumns(void);
extern void RemoveDirectoryTableEntry(Oid relId);

#endif							/* DIRTABLE_H */
