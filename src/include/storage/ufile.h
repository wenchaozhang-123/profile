/*-------------------------------------------------------------------------
 *
 * Unified file abstraction and manipulation.
 *
 * Copyright (c) 2016-Present Hashdata, Inc.
 *
 * src/include/storage/ufile.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef UFILE_H
#define UFILE_H

#include "storage/relfilenode.h"

#define UFILE_ERROR_SIZE	1024

struct UFile;

typedef struct FileAm
{
	struct UFile* (*open) (Oid spcId, const char *fileName, int fileFlags,
						   char *errorMessage, int errorMessageSize);
	void (*close) (struct UFile *file);
	int (*read) (struct UFile *file, char *buffer, int amount);
	int (*write) (struct UFile *file, char *buffer, int amount);
	int64_t (*size) (struct UFile *file);
	void (*unlink) (Oid spcId, const char *fileName);
	char* (*formatFileName) (RelFileNode *relFileNode, const char *fileName);
	bool (*exists) (Oid spcId, const char *fileName);
	const char *(*name) (struct UFile *file);
	const char *(*getLastError) (void);
} FileAm;

typedef struct UFile
{
	FileAm *methods;
} UFile;

extern UFile *UFileOpen(Oid spcId,
						const char *fileName,
						int fileFlags,
						char *errorMessage,
						int errorMessageSize);
extern void UFileClose(UFile *file);

extern int UFileRead(UFile *file, char *buffer, int amount);
extern int UFileWrite(UFile *file, char *buffer, int amount);

extern off_t UFileSize(UFile *file);
extern const char *UFileName(UFile *file);

extern void UFileUnlink(Oid spcId, const char *fileName);
extern char* UFileFormatFileName(RelFileNode *relFileNode, const char *fileName);
extern bool UFileExists(Oid spcId, const char *fileName);

extern const char *UFileGetLastError(UFile *file);

extern struct FileAm localFileAm;

#endif //UFILE_H
