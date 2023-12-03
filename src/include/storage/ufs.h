#ifndef UFS_API_H
#define UFS_API_H

struct UfsIoMethods;

typedef struct UfsFile
{
	struct UfsIoMethods *methods;
} UfsFile;

extern UfsFile *UfsFileOpen(Oid spcId,
							const char *fileName,
							int fileFlags,
							char *errorMessage,
							int errorMessageSize);
extern void UfsFileClose(UfsFile *file);

extern int UfsFilePread(UfsFile *file, char *buffer, int amount, off_t offset);
extern int UfsFilePwrite(UfsFile *file, char *buffer, int amount, off_t offset);

extern int UfsFileRead(UfsFile *file, char *buffer, int amount);
extern int UfsFileWrite(UfsFile *file, char *buffer, int amount);

extern off_t UfsFileSeek(UfsFile *file, off_t offset);
extern off_t UfsFileSize(UfsFile *file);
extern const char *UfsFileName(UfsFile *file);

extern void UfsFileUnlink(Oid spcId, const char *fileName);

extern const char *UfsGetLastError(UfsFile *file);

#endif  /* UFS_API_H */
