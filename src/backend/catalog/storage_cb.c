#include "postgres.h"

#include "access/parallel.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/dirtable.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "common/relpath.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "storage/ufs.h"
#include "storage/ufs_connection.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/spccache.h"
#include "cdb/cdbvars.h"

/*
 * TODO: Redo pending delete
 *
 * We do not support deleteing files during WAL redo, this is because deleting
 * files requires a connection to object storage system. In order to establish
 * the connection to the object storage, we need to access the catalog table to
 * retrieve the connection configuration info, which is impossible during WAL
 * redo.
 *
 */

typedef struct FileNodePendingDelete
{
	RelFileNode node;
	char  relkind;
	Oid   spcId;			/* directory table needs an extra tabpespace */
	char *relativePath;
} FileNodePendingDelete;

typedef struct PendingRelDeleteFile
{
	FileNodePendingDelete filenode;		/* relation that may need to be deleted */
	bool		atCommit;		/* T=delete at commit; F=delete at abort */
	int			nestLevel;		/* xact nesting level of request */
	struct PendingRelDeleteFile *next;		/* linked-list link */
} PendingRelDeleteFile;

static PendingRelDeleteFile *pendingDeleteFiles = NULL; /* head of linked list */

void
DirectoryTableDropStorage(Relation rel)
{
	char *filePath;
	DirectoryTable *dirTable;
	PendingRelDeleteFile *pending;
	const char *server;
	const char *tableSpacePath;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	dirTable = GetDirectoryTable(RelationGetRelid(rel));

	filePath = psprintf("/%s", dirTable->location);

	/* Add the relation to the list of stuff to delete at commit */
	pending = (PendingRelDeleteFile *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDeleteFile));
	pending->filenode.node = rel->rd_node;
	pending->filenode.relkind = rel->rd_rel->relkind;
	pending->filenode.relativePath = MemoryContextStrdup(TopMemoryContext, filePath);
	pending->filenode.spcId = dirTable->spcId;

	pending->atCommit = true;	/* delete if commit */
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingDeleteFiles;

	pendingDeleteFiles = pending;

	pfree(filePath);

	/*
	 * Make sure the connection to the corresponding tablespace has
	 * been cached.
	 *
	 * FileDoDeletesActions->UfsFileUnlink is called outside of the
	 * transaction, if we don't establish a connection here. we may
	 * face the issus of accessing the catalog outside of the
	 * transaction.
	 */
	server = GetDfsTablespaceServer(dirTable->spcId);
	tableSpacePath = GetDfsTablespacePath(dirTable->spcId);
	(void) UfsGetConnection(server, tableSpacePath);
}

void
FileAddCreatePendingEntry(Relation rel, Oid spcId, char *relativePath)
{
	PendingRelDeleteFile *pending;

	/* Add the relation to the list of stuff to delete at abort */
	pending = (PendingRelDeleteFile *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDeleteFile));
	pending->filenode.node = rel->rd_node;
	pending->filenode.relkind = rel->rd_rel->relkind;
	pending->filenode.relativePath = MemoryContextStrdup(TopMemoryContext, relativePath);
	pending->filenode.spcId = spcId;

	pending->atCommit = false;	/* delete if abort */
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingDeleteFiles;

	pendingDeleteFiles = pending;
}

void
FileDoDeletesActions(bool isCommit)
{
	int nestLevel = GetCurrentTransactionNestLevel();
	PendingRelDeleteFile *pending;
	PendingRelDeleteFile *prev;
	PendingRelDeleteFile *next;

	prev = NULL;
	for (pending = pendingDeleteFiles; pending != NULL; pending = next)
	{
		next = pending->next;
		if (pending->nestLevel < nestLevel)
		{
			/* outer-level entries should not be processed yet */
			prev = pending;
		}
		else
		{
			/* unlink list entry first, so we don't retry on failure */
			if (prev)
				prev->next = next;
			else
				pendingDeleteFiles = next;

			/* do deletion if called for */
			if (pending->atCommit == isCommit)
				UfsFileUnlink(pending->filenode.spcId, pending->filenode.relativePath);

			/* must explicitly free the list entry */
			if (pending->filenode.relativePath)
				pfree(pending->filenode.relativePath);

			pfree(pending);
			/* prev does not change */
		}
	}
}

void
FileAtSubCommitSmgr(void)
{
	int	nestLevel = GetCurrentTransactionNestLevel();
	PendingRelDeleteFile *pending;

	for (pending = pendingDeleteFiles; pending != NULL; pending = pending->next)
	{
		if (pending->nestLevel >= nestLevel)
			pending->nestLevel = nestLevel - 1;
	}
}

void
FileAtSubAbortSmgr(void)
{
	FileDoDeletesActions(false);
}
