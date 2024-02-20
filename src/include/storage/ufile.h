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

#ifndef FILE_MANIP_H
#define FILE_MANIP_H

#include "storage/relfilenode.h"

struct FileAm;
typedef struct UFile
{
	struct FileAm *methods;
} UFile;

extern UFile *UFileOpen(RelFileNode *relFileNode,
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
extern bool UFileExists(Oid spcId, const char *fileName);

extern const char *UFileGetLastError(UFile *file);

#endif //FILE_MANIP_H
