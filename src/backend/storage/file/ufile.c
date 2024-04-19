/*-------------------------------------------------------------------------
 *
 * Unified file abstraction and manipulation.
 *
 * Copyright (c) 2016-Present Hashdata, Inc.
 *
 *
 *  * IDENTIFICATION
 *	  src/backend/storage/file/ufile.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog/pg_directory_table.h"
#include "catalog/pg_tablespace.h"
#include "cdb/cdbvars.h"
#include "commands/tablespace.h"
#include "common/relpath.h"
#include "storage/ufile.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"
#include "utils/elog.h"
#include "utils/wait_event.h"

typedef struct LocalFile
{
	FileAm * methods;
	File file;
	off_t offset;
} LocalFile;

static UFile *localFileOpen(Oid spcId, const char *fileName, int fileFlags,
							char *errorMessage, int errorMessageSize);
static void localFileClose(UFile *file);
static int	localFileRead(UFile *file, char *buffer, int amount);
static int	localFileWrite(UFile *file, char *buffer, int amount);
static off_t localFileSize(UFile *file);
static void localFileUnlink(Oid spcId, const char *fileName);
static char *localFormatPathName(RelFileNode *relFileNode);
static bool localFileExists(Oid spcId, const char *fileName);
static const char *localFileName(UFile *file);
static const char *localGetLastError(void);

static char localFileErrorStr[UFILE_ERROR_SIZE];

struct FileAm localFileAm = {
	.open = localFileOpen,
	.close = localFileClose,
	.read = localFileRead,
	.write = localFileWrite,
	.size = localFileSize,
	.unlink = localFileUnlink,
	.formatPathName = localFormatPathName,
	.exists = localFileExists,
	.name = localFileName,
	.getLastError = localGetLastError,
};

static UFile *
localFileOpen(Oid spcId,
			  const char *fileName,
			  int fileFlags,
			  char *errorMessage,
			  int errorMessageSize)
{
	LocalFile *result;
	File file;

	file = PathNameOpenFile(fileName, fileFlags);
	if (file < 0)
	{
		snprintf(errorMessage, errorMessageSize, "%s", strerror(errno));
		return NULL;
	}

	result = palloc0(sizeof(LocalFile));
	result->methods = &localFileAm;
	result->file = file;
	result->offset = 0;

	return (UFile *) result;
}

static void
localFileClose(UFile *file)
{
	LocalFile *localFile = (LocalFile *) file;

	FileClose(localFile->file);
}

static int
localFilePread(UFile *file, char *buffer, int amount, off_t offset)
{
	int bytes;
	LocalFile *localFile = (LocalFile *) file;

	localFile->offset = offset;
	bytes = FileRead(localFile->file, buffer, amount, offset, WAIT_EVENT_DATA_FILE_READ);
	if (bytes < 0)
	{
		snprintf(localFileErrorStr, UFILE_ERROR_SIZE, "%s", strerror(errno));
		return -1;
	}

	localFile->offset += bytes;
	return bytes;
}

static int
localFileRead(UFile *file, char *buffer, int amount)
{
	LocalFile *localFile = (LocalFile *) file;

	return localFilePread(file, buffer, amount, localFile->offset);
}

static int
localFilePwrite(UFile *file, char *buffer, int amount, off_t offset)
{
	int bytes;
	LocalFile *localFile = (LocalFile *) file;

	localFile->offset = offset;
	bytes = FileWrite(localFile->file, buffer, amount, offset, WAIT_EVENT_DATA_FILE_WRITE);
	if (bytes < 0)
	{
		snprintf(localFileErrorStr, UFILE_ERROR_SIZE, "%s", strerror(errno));
		return -1;
	}

	localFile->offset += bytes;
	return bytes;
}

static int
localFileWrite(UFile *file, char *buffer, int amount)
{
	LocalFile *localFile = (LocalFile *) file;

	return localFilePwrite(file, buffer, amount, localFile->offset);
}

static off_t
localFileSize(UFile *file)
{
	LocalFile *localFile = (LocalFile *) file;

	return FileSize(localFile->file);
}

static bool
destory_local_file_directories(const char* directoryName)
{
	DIR		   *dirdesc;
	struct dirent *de;
	char	   *subfile;
	struct stat st;

	Assert(LWLockHeldByMe(DirectoryTableLock));

	elog(DEBUG5, "destory_local_file_directories for directory %s",
		 directoryName);

	dirdesc = AllocateDir(directoryName);
	if (dirdesc == NULL)
	{
		if (errno == ENOENT)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open directory \"%s\": %m",
							directoryName)));
			/* The symlink might still exist, so go try to remove it */
			return false;
		}
	}

	while ((de = ReadDir(dirdesc, directoryName)) != NULL)
	{
		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		subfile = psprintf("%s/%s", directoryName, de->d_name);

		if (stat(subfile, &st) == 0 &&
			S_ISDIR(st.st_mode))
		{
			/* remove directory and file recursively */

			if (!destory_local_file_directories(subfile))
			{
				ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("directories for directory table \"%s\" could not be removed: %m",
			   						subfile),
							 errhint("You can remove the directories manually if necessary.")));

				FreeDir(dirdesc);
				pfree(subfile);

				return false;
			}
		}
		else
		{
			/* remove file */
			if (unlink(subfile) < 0)
				ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not remove files \"%s\": %m",
								   subfile)));
		}

		pfree(subfile);
	}

	FreeDir(dirdesc);

	/* remove directory */
	if (rmdir(directoryName) < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove directory \"%s\": %m",
						directoryName)));
	}

	return true;
}

static void
localFileUnlink(Oid spcId, const char *fileName)
{
	struct stat st;

	LWLockAcquire(DirectoryTableLock, LW_EXCLUSIVE);
	if (stat(fileName, &st) == 0 &&
		S_ISDIR(st.st_mode))
	{
		/* remove directory and file recursively */
		if (!destory_local_file_directories(fileName))
			ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("directories for directory table \"%s\" could not be removed: %m",
							   fileName),
						 errhint("You can remove the directories manually if necessary.")));
		LWLockRelease(DirectoryTableLock);
	}
	else
	{
		LWLockRelease(DirectoryTableLock);
		/* remove file */
		if (unlink(fileName) < 0)
		{
			if (errno != ENOENT)
				ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not remove file \"%s\": %m", fileName)));
		}
	}

}

static char *
localFormatPathName(RelFileNode *relFileNode)
{
	if (relFileNode->spcNode == DEFAULTTABLESPACE_OID)
		return psprintf("base/%u/"UINT64_FORMAT"_dirtable/",
				  		relFileNode->dbNode, relFileNode->relNode);
	else
		return psprintf("pg_tblspc/%u/%s/%u/"UINT64_FORMAT"_dirtable/",
						relFileNode->spcNode, GP_TABLESPACE_VERSION_DIRECTORY,
						relFileNode->dbNode, relFileNode->relNode);
}

static bool
localFileExists(Oid spcId, const char *fileName)
{
	struct stat fileStats;

	if (stat(fileName, &fileStats) != 0)
	{
		if (errno == ENOENT)
			return false;

		ereport(ERROR,
					(errcode_for_file_access(),
				 	 errmsg("unable to stat file \"%s\": %m", fileName)));
	}

	return true;
}

static const char *
localFileName(UFile *file)
{
	LocalFile *localFile = (LocalFile *) file;

	return FilePathName(localFile->file);
}

static const char *
localGetLastError(void)
{
	return localFileErrorStr;
}

UFile *
UFileOpen(Oid spcId,
		  const char *fileName,
		  int fileFlags,
		  char *errorMessage,
		  int errorMessageSize)
{
	UFile *ufile;
	FileAm *fileAm;

	fileAm = GetTablespaceFileHandler(spcId);
	ufile = fileAm->open(spcId, fileName, fileFlags, errorMessage, errorMessageSize);

	return ufile;
}

void
UFileClose(UFile *file)
{
	file->methods->close(file);
	//TODO pfree move to close
	pfree(file);
}

int
UFileRead(UFile *file, char *buffer, int amount)
{
	return file->methods->read(file, buffer, amount);
}

int
UFileWrite(UFile *file, char *buffer, int amount)
{
	return file->methods->write(file, buffer, amount);
}

int64_t
UFileSize(UFile *file)
{
	return file->methods->size(file);
}

const char *
UFileName(UFile *file)
{
	return file->methods->name(file);
}

void
UFileUnlink(Oid spcId, const char *fileName)
{
	FileAm *fileAm;

	fileAm = GetTablespaceFileHandler(spcId);

	fileAm->unlink(spcId, fileName);
}

char *
UFileFormatPathName(RelFileNode *relFileNode)
{
	FileAm *fileAm;

	fileAm = GetTablespaceFileHandler(relFileNode->spcNode);

	return fileAm->formatPathName(relFileNode);
}

bool
UFileExists(Oid spcId, const char *fileName)
{
	FileAm *fileAm;

	fileAm = GetTablespaceFileHandler(spcId);

	return fileAm->exists(spcId, fileName);
}

const char *
UFileGetLastError(UFile *file)
{
	return file->methods->getLastError();
}

