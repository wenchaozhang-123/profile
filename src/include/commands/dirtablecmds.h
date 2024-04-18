/*-------------------------------------------------------------------------
 *
 * dirtablecmds.h
 *	  prototypes for dirtablecmds.c.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/dirtablecmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DIRTABLECMDS_H
#define DIRTABLECMDS_H

#include "catalog/pg_directory_table.h"
#include "catalog/objectaddress.h"
#include "nodes/params.h"
#include "parser/parse_node.h"

extern void CreateDirectoryTable(CreateDirectoryTableStmt *stmt, Oid relId);
extern char *GetScopedFileUrl(DirectoryTable *dirTable, char *relativePath);
extern Datum GetFileContent(Oid spcId, char *scopedFileUrl);

#endif //DIRTABLECMDS_H
