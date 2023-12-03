#include "postgres.h"

#include <unistd.h>
#include "catalog/pg_tablespace_d.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"
#include "storage/ufs_connection.h"
#include "storage/ufs.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"
#include "utils/spccache.h"

#define LOCAL_FILE_ERROR_SIZE   1024
#define REMOTE_FILE_ERROR_SIZE  1024
#define REMOTE_FILE_BLOCK_SIZE (1024 * 1024 * 8)

typedef struct UfsIoMethods {
	void (*close) (UfsFile *file);
	int (*pread) (UfsFile *file, char *buffer, int amount, off_t offset);
	int (*pwrite) (UfsFile *file, char *buffer, int amount, off_t offset);
	int (*read) (UfsFile *file, char *buffer, int amount);
	int (*write) (UfsFile *file, char *buffer, int amount);
	off_t (*seek) (UfsFile *file, off_t offset);
	int64_t (*size) (UfsFile *file);
	const char *(*name) (UfsFile *file);
	const char *(*getLastError)(void);
} UfsIoMethods;

typedef struct LocalFile {
	struct UfsIoMethods *methods;
	File file;
	off_t offset;
} LocalFile;

typedef struct RemoteFileHandle
{
	gopherFS fs;
	gopherFile file;
	ResourceOwner owner;
	struct RemoteFileHandle *next;
	struct RemoteFileHandle *prev;
} RemoteFileHandle;

typedef struct RemoteFileEx {
	struct UfsIoMethods *methods;
	char *fileName;
	RemoteFileHandle *handle;
} RemoteFileEx;

static void localFileClose(UfsFile *file);
static void remoteFileClose(UfsFile *file);

static int localFilePread(UfsFile *file,char *buffer, int amount, off_t offset);
static int localFileRead(UfsFile *file,char *buffer, int amount);
static int remoteFilePread(UfsFile *file, char *buffer, int amount, off_t offset);
static int remoteFileRead(UfsFile *file, char *buffer, int amount);

static int localFilePwrite(UfsFile *file, char *buffer, int amount, off_t offset);
static int localFileWrite(UfsFile *file, char *buffer, int amount);
static int remoteFilePwrite(UfsFile *file, char *buffer, int amount, off_t offset);
static int remoteFileWrite(UfsFile *file, char *buffer, int amount);

static off_t localFileSeek(UfsFile *file, off_t offset);
static off_t remoteFileSeek(UfsFile *file, off_t offset);

static off_t localFileSize(UfsFile *file);
static off_t remoteFileSize(UfsFile *file);

static void localFileUnlink(const char *fileName);
static void remoteFileUnlink(gopherFS connection, const char *fileName);

static const char *localFileName(UfsFile *file);
static const char *remoteFileName(UfsFile *file);

static const char *localGetLastError(void);
static const char *remoteGetLastError(void);

static char localFileErrorStr[LOCAL_FILE_ERROR_SIZE];
static char remoteFileErrorStr[REMOTE_FILE_ERROR_SIZE];

static UfsIoMethods ufsIoMethods[] = {
	{
		.close = localFileClose,
		.pread = localFilePread,
		.pwrite = localFilePwrite,
		.read = localFileRead,
		.write = localFileWrite,
		.seek = localFileSeek,
		.size = localFileSize,
		.name = localFileName,
		.getLastError = localGetLastError,
	},
	{
		.close = remoteFileClose,
		.pread = remoteFilePread,
		.pwrite = remoteFilePwrite,
		.read = remoteFileRead,
		.write = remoteFileWrite,
		.seek = remoteFileSeek,
		.size = remoteFileSize,
		.name = remoteFileName,
		.getLastError = remoteGetLastError,
	}
};

static RemoteFileHandle *openRemoteHandles;
static bool resownerCallbackRegistered;

static RemoteFileHandle *
createRemoteFileHandle(void)
{
	RemoteFileHandle *result;

	result = MemoryContextAlloc(TopMemoryContext, sizeof(RemoteFileHandle));

	result->fs = NULL;
	result->file = NULL;
	result->prev = NULL;
	result->next = openRemoteHandles;
	result->owner = CurrentResourceOwner;

	if (openRemoteHandles)
		openRemoteHandles->prev = result;

	openRemoteHandles = result;

	return result;
}

static void
destroyRemoteFileHandle(RemoteFileHandle *handle)
{
	if (handle->prev)
		handle->prev->next = handle->next;
	else
		openRemoteHandles = openRemoteHandles->next;

	if (handle->next)
		handle->next->prev = handle->prev;

	if (handle->file)
		gopherCloseFile(handle->fs, handle->file, true);

	pfree(handle);
}

static void
remoteFileAbortCallback(ResourceReleasePhase phase,
						bool isCommit,
						bool isTopLevel,
						void *arg)
{
	RemoteFileHandle *curr;
	RemoteFileHandle *next;

	if (phase != RESOURCE_RELEASE_AFTER_LOCKS)
		return;

	next = openRemoteHandles;
	while (next)
	{
		curr = next;
		next = curr->next;

		if (curr->owner == CurrentResourceOwner)
		{
			if (isCommit)
				elog(LOG, "remoteFile reference leak: %p still referenced", curr);

			destroyRemoteFileHandle(curr);
		}
	}
}

static void
localFileClose(UfsFile *file)
{
	LocalFile *localFile = (LocalFile *) file;

	FileClose(localFile->file);
}

static void
remoteFileClose(UfsFile *file)
{
	RemoteFileEx *remoteFile = (RemoteFileEx *) file;

	destroyRemoteFileHandle(remoteFile->handle);

	pfree(remoteFile->fileName);
}

static int
localFilePread(UfsFile *file, char *buffer, int amount, off_t offset)
{
	int bytes;
	LocalFile *localFile = (LocalFile *) file;

	localFile->offset = offset;
	bytes = FileRead(localFile->file, buffer, amount, offset, WAIT_EVENT_DATA_FILE_READ);
	if (bytes < 0)
	{
		snprintf(localFileErrorStr, LOCAL_FILE_ERROR_SIZE, "%s", strerror(errno));
		return -1;
	}

	localFile->offset += bytes;
	return bytes;
}

static int
localFileRead(UfsFile *file, char *buffer, int amount)
{
	LocalFile *localFile = (LocalFile *) file;

	return localFilePread(file, buffer, amount, localFile->offset);
}

static int
remoteFileReadInternal(RemoteFileEx *remoteFile, char *buffer, int amount, off_t offset)
{
	int bytes;

	if (gopherSeek(remoteFile->handle->fs, remoteFile->handle->file, offset) < 0)
		goto failed;

	bytes = gopherRead(remoteFile->handle->fs, remoteFile->handle->file, buffer, amount);
	if (bytes < 0)
		goto failed;

	return bytes;

failed:
	snprintf(remoteFileErrorStr, REMOTE_FILE_ERROR_SIZE, "%s", gopherGetLastError());
	return -1;
}

static int
remoteFilePread(UfsFile *file, char *buffer, int amount, off_t offset)
{
	int bytes;
	RemoteFileEx *remoteFile = (RemoteFileEx *) file;

	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
	bytes = remoteFileReadInternal(remoteFile, buffer, amount, offset);
	pgstat_report_wait_end();

	return bytes;
}

static int
remoteFileRead(UfsFile *file, char *buffer, int amount)
{
	int bytes;
	RemoteFileEx *remoteFile = (RemoteFileEx *) file;

	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
	bytes = gopherRead(remoteFile->handle->fs, remoteFile->handle->file, buffer, amount);
	pgstat_report_wait_end();

	if (bytes < 0)
	{
		snprintf(remoteFileErrorStr, REMOTE_FILE_ERROR_SIZE, "%s", gopherGetLastError());
		return -1;
	}

	return bytes;
}

static int
localFilePwrite(UfsFile *file, char *buffer, int amount, off_t offset)
{
	int bytes;
	LocalFile *localFile = (LocalFile *) file;

	localFile->offset = offset;
	bytes = FileWrite(localFile->file, buffer, amount, offset, WAIT_EVENT_DATA_FILE_WRITE);
	if (bytes < 0)
	{
		snprintf(localFileErrorStr, LOCAL_FILE_ERROR_SIZE, "%s", strerror(errno));
		return -1;
	}

	localFile->offset += bytes;
	return bytes;
}

static int
localFileWrite(UfsFile *file, char *buffer, int amount)
{
	LocalFile *localFile = (LocalFile *) file;

	return localFilePwrite(file, buffer, amount, localFile->offset);
}

static int
remoteFileWriteInternal(RemoteFileEx *remoteFile, char *buffer, int amount, off_t offset)
{
	int bytes;

	/* NB: Gopher does not support seek operator in write mode */
#if 0
	if (gopherSeek(remoteFile->handle->fs, remoteFile->handle->file, offset) < 0)
		return -1;
#endif

	bytes = gopherWrite(remoteFile->handle->fs, remoteFile->handle->file, buffer, amount);
	if (bytes < 0)
	{
		snprintf(remoteFileErrorStr, REMOTE_FILE_ERROR_SIZE, "%s", gopherGetLastError());
		return -1;
	}

	return bytes;
}

static int
remoteFilePwrite(UfsFile *file, char *buffer, int amount, off_t offset)
{
	int bytes;
	RemoteFileEx *remoteFile = (RemoteFileEx *) file;

	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
	bytes = remoteFileWriteInternal(remoteFile, buffer, amount, offset);
	pgstat_report_wait_end();

	return bytes;
}

static int
remoteFileWrite(UfsFile *file, char *buffer, int amount)
{
	int bytes;
	RemoteFileEx *remoteFile = (RemoteFileEx *) file;

	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
	bytes = gopherWrite(remoteFile->handle->fs, remoteFile->handle->file, buffer, amount);
	pgstat_report_wait_end();

	if (bytes < 0)
	{
		snprintf(remoteFileErrorStr, REMOTE_FILE_ERROR_SIZE, "%s", gopherGetLastError());
		return -1;
	}

	return bytes;
}

static off_t
localFileSeek(UfsFile *file, off_t offset)
{
	LocalFile *localFile = (LocalFile *) file;

	if (offset < 0)
	{
		snprintf(localFileErrorStr, LOCAL_FILE_ERROR_SIZE, "invalid offset %ld", offset);
		return -1;
	}

	localFile->offset = offset;
	return offset;
}

static off_t
remoteFileSeek(UfsFile *file, off_t offset)
{
	RemoteFileEx *remoteFile = (RemoteFileEx *) file;

	if (gopherSeek(remoteFile->handle->fs, remoteFile->handle->file, offset) < 0)
	{
		snprintf(remoteFileErrorStr, REMOTE_FILE_ERROR_SIZE, "%s", gopherGetLastError());
		return -1;
	}

	return offset;
}

static off_t
localFileSize(UfsFile *file)
{
	LocalFile *localFile = (LocalFile *) file;

	return FileSize(localFile->file);
}

static off_t
remoteFileSize(UfsFile *file)
{
	off_t result;
	RemoteFileEx *remoteFile = (RemoteFileEx *) file;

	gopherFileInfo* fileInfo = gopherGetFileInfo(remoteFile->handle->fs, remoteFile->fileName);
	if (fileInfo == NULL)
	{
		snprintf(remoteFileErrorStr, REMOTE_FILE_ERROR_SIZE, "%s", gopherGetLastError());
		return -1;
	}

	result = fileInfo->mLength;
	gopherFreeFileInfo(fileInfo, 1);

	return result; 
}

static const char *
localFileName(UfsFile *file)
{
	LocalFile *localFile = (LocalFile *) file;

	return FilePathName(localFile->file);
}

static const char *
remoteFileName(UfsFile *file)
{
	RemoteFileEx *remoteFile = (RemoteFileEx *) file;

	return remoteFile->fileName;
}

static const char *
localGetLastError(void)
{
	return localFileErrorStr;
}

static const char *
remoteGetLastError(void)
{
	return remoteFileErrorStr;
}

static UfsFile *
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

	result = palloc(sizeof(LocalFile));
	result->methods = &ufsIoMethods[0];
	result->file = file;
	result->offset = 0;

	return (UfsFile *) result;
}

static UfsFile *
remoteFileOpen(Oid spcId,
			   const char *fileName,
			   int fileFlags,
			   char *errorMessage,
			   int errorMessageSize)
{
	RemoteFileEx *result;
	gopherFS connection;
	gopherFile gopherFile;
	const char *server;
	const char *tableSpacePath;
	bool hasError = false;
	ErrorData *errData;
	MemoryContext mcxt = CurrentMemoryContext;

	PG_TRY();
	{
		server = GetDfsTablespaceServer(spcId);
		tableSpacePath = GetDfsTablespacePath(spcId);
		connection = UfsGetConnection(server, tableSpacePath);

		if (!resownerCallbackRegistered)
		{
			RegisterResourceReleaseCallback(remoteFileAbortCallback, NULL);
			resownerCallbackRegistered = true;
		}

		result = palloc(sizeof(RemoteFileEx));
		result->methods = &ufsIoMethods[1];
		result->fileName = pstrdup(fileName);
		result->handle = createRemoteFileHandle();
	}
	PG_CATCH();
	{
		hasError = true;
		MemoryContextSwitchTo(mcxt);
		errData = CopyErrorData();
		FlushErrorState();
		strlcpy(errorMessage, errData->message, errorMessageSize);
		FreeErrorData(errData);
	}
	PG_END_TRY();

	if (hasError)
		return NULL;

	gopherFile = gopherOpenFile(connection, result->fileName, fileFlags, REMOTE_FILE_BLOCK_SIZE);
	if (gopherFile == NULL)
	{
		snprintf(errorMessage, errorMessageSize, "%s", gopherGetLastError());
		return NULL;
	}

	result->handle->fs = connection;
	result->handle->file = gopherFile;
	return (UfsFile *) result;
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

static void
remoteFileUnlink(gopherFS connection, const char *fileName)
{
	if (gopherPrefixDelete(connection, fileName) < 0)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %s", fileName, gopherGetLastError())));
}

UfsFile *
UfsFileOpen(Oid spcId,
			const char *fileName,
			int fileFlags,
			char *errorMessage,
			int errorMessageSize)
{
	bool isDfsTableSpace;

	isDfsTableSpace = IsDfsTablespaceById(spcId);
	if (isDfsTableSpace)
		return remoteFileOpen(spcId,
							  fileName,
							  fileFlags,
							  errorMessage,
							  errorMessageSize);

	return localFileOpen(spcId,
						 fileName,
						 fileFlags,
						 errorMessage,
						 errorMessageSize);
}

void
UfsFileClose(UfsFile *file)
{
	file->methods->close(file);
	pfree(file);
}

int
UfsFilePread(UfsFile *file, char *buffer, int amount, off_t offset)
{
	return file->methods->pread(file, buffer, amount, offset);
}

int
UfsFilePwrite(UfsFile *file, char *buffer, int amount, off_t offset)
{
	return file->methods->pwrite(file, buffer, amount, offset);
}

int
UfsFileRead(UfsFile *file, char *buffer, int amount)
{
	return file->methods->read(file, buffer, amount);
}

int
UfsFileWrite(UfsFile *file, char *buffer, int amount)
{
	return file->methods->write(file, buffer, amount);
}

off_t
UfsFileSeek(UfsFile *file, off_t offset)
{
	return file->methods->seek(file, offset);
}

off_t
UfsFileSize(UfsFile *file)
{
	return file->methods->size(file);
}

const char *
UfsFileName(UfsFile *file)
{
	return file->methods->name(file);
}

void
UfsFileUnlink(Oid spcId, const char *fileName)
{
	bool isDfsTableSpace;

	isDfsTableSpace = IsDfsTablespaceById(spcId);
	if (isDfsTableSpace)
	{
		const char *server = GetDfsTablespaceServer(spcId);
		const char *tableSpacePath = GetDfsTablespacePath(spcId);
		gopherFS connection = UfsGetConnection(server, tableSpacePath);

		return remoteFileUnlink(connection, fileName);
	}

	localFileUnlink(fileName);
}

const char *
UfsGetLastError(UfsFile *file)
{
	return file->methods->getLastError();
}
