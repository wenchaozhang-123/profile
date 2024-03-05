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

#include "catalog/pg_tablespace.h"
#include "common/relpath.h"
#include "storage/ufile.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"
#include "utils/elog.h"
#include "utils/wait_event.h"

#define UFILE_ERROR_SIZE	1024

typedef struct FileAm
{
	void (*close) (UFile *file);
	int (*read) (UFile *file, char *buffer, int amount);
	int (*write) (UFile *file, char *buffer, int amount);
	int64_t (*size) (UFile *file);
	const char *(*name) (UFile *file);
	const char *(*getLastError) (void);
} FileAm;

typedef struct LocalFile
{
	struct FileAm * methods;
	File file;
	off_t offset;
} LocalFile;

static UFile *localFileOpen(Oid spcId, const char *fileName,
							int fileFlags, char *errorMessage, int errorMessageSize);
static void localFileClose(UFile *file);
static int	localFileRead(UFile *file, char *buffer, int amount);
static int	localFileWrite(UFile *file, char *buffer, int amount);
static off_t localFileSize(UFile *file);
static void localFileUnlink(const char *fileName);
static bool localFileExists(const char *fileName);
static const char *localFileName(UFile *file);
static const char *localGetLastError(void);

//static char *formatLocalFileName(RelFileNode *relFileNode, const char *fileName);

static char localFileErrorStr[UFILE_ERROR_SIZE];

static FileAm localFileAm = {
	.close = localFileClose,
	.read = localFileRead,
	.write = localFileWrite,
	.size = localFileSize,
	.name = localFileName,
	.getLastError = localGetLastError,
};

//TODO
//static FileAm *currentFileAm = &localFileAm;

static UFile *
UFileOpenInternal(Oid spcId,
				  bool isNormalfile,
				  const char *fileName,
				  int fileFlags,
				  char *errorMessage,
				  int errorMessageSize)
{
//	bool isDfsTableSpace;

//	isDfsTableSpace = IsDfsTablespace(relFileNode->spcNode);
//	if (isDfsTableSpace)
//		return remoteFileOpen(relFileNode->spcNode,
//							  isNormalfile,
//							  fileName,
//							  fileFlags,
//							  errorMessage,
//							  errorMessageSize);

	return localFileOpen(spcId,
						 fileName,
						 fileFlags,
						 errorMessage,
						 errorMessageSize);
}

UFile *
UFileOpen(Oid spcId,
		  const char *fileName,
		  int fileFlags,
		  char *errorMessage,
		  int errorMessageSize)
{
	return UFileOpenInternal(spcId,
							 true,
							 fileName,
							 fileFlags,
							 errorMessage,
							 errorMessageSize);
}

static UFile *
localFileOpen(Oid spcId,
			  const char *fileName,
			  int fileFlags,
			  char *errorMessage,
			  int errorMessageSize)
{
	//char *filePath;
	LocalFile *result;
	File file;

	//filePath = formatLocalFileName(relFileNode, fileName);

	//file = PathNameOpenFile(filePath, fileFlags);
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

	//pfree(filePath);

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

static void
localFileUnlink(const char *fileName)
{
	if (unlink(fileName) < 0)
	{
		if (errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
						errmsg("could not remove file \"%s\": %m", fileName)));
	}
}

static bool
localFileExists(const char *fileName)
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

//static char *
//formatLocalFileName(RelFileNode *relFileNode, const char *fileName)
//{
//	if (relFileNode->spcNode == DEFAULTTABLESPACE_OID)
//		return psprintf("base/%u/%s", relFileNode->dbNode, fileName);
//	else
//		return psprintf("pg_tblspc/%u/%s/%u/%s",
//						relFileNode->spcNode, GP_TABLESPACE_VERSION_DIRECTORY,
//		relFileNode->dbNode, fileName);
//}

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


// TODO dfs
static void
UFileUnlinkInternal(Oid spcId, bool isNormalFile, const char *fileName)
{
	localFileUnlink(fileName);
}

void
UFileUnlink(Oid spcId, const char *fileName)
{
	return UFileUnlinkInternal(spcId, true, fileName);
}

bool
UFileExists(Oid spcId, const char *fileName)
{
//	bool isDfsTableSpace;
//
//	isDfsTableSpace = IsDfsTablespaceById(spcId);
//	if (isDfsTableSpace)
//	{
//		const char *server = GetDfsTablespaceServer(spcId);
//		const char *tableSpacePath = GetDfsTablespacePath(spcId);
//		gopherFS connection = UfsGetConnection(server, tableSpacePath);
//
//		return remoteFileExists(connection, fileName);
//	}

	return localFileExists(fileName);
}

const char *
UFileGetLastError(UFile *file)
{
	return file->methods->getLastError();
}

char *
formatLocalFileName(RelFileNode *relFileNode, const char *fileName)
{
	if (relFileNode->spcNode == DEFAULTTABLESPACE_OID)
		return psprintf("base/%u/%s", relFileNode->dbNode, fileName);
	else
		return psprintf("pg_tblspc/%u/%s/%u/%s",
						relFileNode->spcNode, GP_TABLESPACE_VERSION_DIRECTORY,
		relFileNode->dbNode, fileName);
}
