/*-------------------------------------------------------------------------
 *
 * storagecmds.c
 *	  storage server/user_mapping creation/manipulation commands
 *
 * Copyright (c) 2016-Present Hashdata, Inc.
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/storagecmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/gp_storage_server.h"
#include "catalog/objectaccess.h"
#include "catalog/oid_dispatch.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "commands/storagecmds.h"
#include "executor/execdesc.h"
#include "nodes/pg_list.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

Oid
get_storage_server_oid(const char *servername, bool missing_ok)
{
	Oid 		oid;

	oid = GetSysCacheOid1(STORAGESERVERNAME, Anum_gp_storage_server_oid,
					   		CStringGetDatum(servername));

	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("server \"%s\" does not exist", servername)));
	return oid;
}

/*
 * GetStorageServerExtended - look up the storage server definition. If
 * flags uses FSV_MISSING_OK, return NULL if the object cannot be found
 * instead of raising an error.
 */
StorageServer *
GetStorageServerExtended(Oid serverid, bits16 flags)
{
	Form_gp_storage_server serverform;
	StorageServer *server;
	HeapTuple	tp;
	Datum		datum;
	bool		isnull;

	tp = SearchSysCache1(STORAGESERVEROID, ObjectIdGetDatum(serverid));

	if (!HeapTupleIsValid(tp))
	{
		if ((flags & SSV_MISSING_OK) == 0)
			elog(ERROR, "cache lookup failed for storage server %u", serverid);
		return NULL;
	}

	serverform = (Form_gp_storage_server) GETSTRUCT(tp);

	server = (StorageServer *) palloc(sizeof(StorageServer));
	server->serverid = serverid;
	server->servername = pstrdup(NameStr(serverform->srvname));
	server->owner = serverform->srvowner;

	/* Extract the srvoptions */
	datum = SysCacheGetAttr(STORAGESERVEROID,
							tp,
							Anum_gp_storage_server_srvoptions,
							&isnull);
	if (isnull)
		server->options = NIL;
	else
		server->options = untransformRelOptions(datum);

	ReleaseSysCache(tp);

	return server;
}

/*
 * GetStorageServer - look up the storage server definition.
 */
StorageServer *
GetStorageServer(Oid serverid)
{
	return GetStorageServerExtended(serverid, 0);
}

/*
 * GetStorageServerByName - look up the storage server definition by name.
 */
StorageServer *
GetStorageServerByName(const char *srvname, bool missing_ok)
{
	Oid			serverid = get_storage_server_oid(srvname, missing_ok);

	if (!OidIsValid(serverid))
		return NULL;

	return GetStorageServer(serverid);
}

/*
 * Convert a DefElem list to the text array format that is used in
 * gp_storage_server, gp_storage_user_mapping.
 *
 * Returns the array in the form of a Datum, or PointerGetDatum(NULL)
 * if the list is empty.
 *
 * Note: The array is usually stored to database without further
 * processing, hence any validation should be done before this
 * conversion.
 */
static Datum
optionListToArray(List *options)
{
	ArrayBuildState *astate = NULL;
	ListCell   *cell;

	foreach(cell, options)
	{
		DefElem    *def = lfirst(cell);
		const char *value;
		Size		len;
		text	   *t;

		value = defGetString(def);
		len = VARHDRSZ + strlen(def->defname) + 1 + strlen(value);
		t = palloc(len + 1);
		SET_VARSIZE(t, len);
		sprintf(VARDATA(t), "%s=%s", def->defname, value);

		astate = accumArrayResult(astate, PointerGetDatum(t),
								  false, TEXTOID,
								  CurrentMemoryContext);
	}

	if (astate)
		return makeArrayResult(astate, CurrentMemoryContext);

	return PointerGetDatum(NULL);
}

/*
 * Transform a list of DefElem into text array format.  This is substantially
 * the same thing as optionListToArray(), except we recognize SET/ADD/DROP
 * actions for modifying an existing list of options, which is passed in
 * Datum form as oldOptions.
 *
 * Returns the array in the form of a Datum, or PointerGetDatum(NULL)
 * if the list is empty.
 *
 * This is used by CREATE/ALTER of STORAGE SERVER/USER MAPPING
 */
Datum
transformStorageGenericOptions(Oid catalogId,
						Datum oldOptions,
						List *options)
{
	List	   *resultOptions = untransformRelOptions(oldOptions);
	ListCell   *optcell;
	Datum		result;

	foreach(optcell, options)
	{
		DefElem    *od = lfirst(optcell);
		ListCell   *cell;

		/*
		 * Find the element in resultOptions.  We need this for validation in
		 * all cases.
		 */
		foreach(cell, resultOptions)
		{
			DefElem    *def = lfirst(cell);

			if (strcmp(def->defname, od->defname) == 0)
				break;
		}

		/*
		 * It is possible to perform multiple SET/DROP actions on the same
		 * option.  The standard permits this, as long as the options to be
		 * added are unique.  Note that an unspecified action is taken to be
		 * ADD.
		 */
		switch (od->defaction)
		{
			case DEFELEM_DROP:
				if (!cell)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("option \"%s\" not found",
									   od->defname)));
				resultOptions = list_delete_cell(resultOptions, cell);
				break;

			case DEFELEM_SET:
				if (!cell)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("option \"%s\" not found",
									   od->defname)));
				lfirst(cell) = od;
				break;

			case DEFELEM_ADD:
			case DEFELEM_UNSPEC:
				if (cell)
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("option \"%s\" provided more than once",
									   od->defname)));
				resultOptions = lappend(resultOptions, od);
				break;

			default:
				elog(ERROR, "unrecognized action %d on option \"%s\"",
					 (int) od->defaction, od->defname);
				break;
		}
	}

	result = optionListToArray(resultOptions);

	return result;
}

/*
 * Create a storage server
 */
ObjectAddress
CreateStorageServer(CreateStorageServerStmt *stmt)
{
	Relation 	rel;
	Datum 		srvoptions;
	Datum 		values[Natts_gp_storage_server];
	bool 		nulls[Natts_gp_storage_server];
	HeapTuple 	tuple;
	Oid 		srvId;
	Oid 		ownerId;
	ObjectAddress	myself = {0};

	rel = table_open(StorageServerRelationId, RowExclusiveLock);

	/* For now the owner cannot be specified on create. Use effective user ID. */
	ownerId = GetUserId();

	/*
	 * Check that there is no other foreign server by this name. Do nothing if
	 * IF NOT EXISTS was enforced.
	 */
	if (GetStorageServerByName(stmt->servername, true) != NULL)
	{
		if (stmt->if_not_exists)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					errmsg("storage server \"%s\" already exists, skipping",
							stmt->servername)));
			table_close(rel, RowExclusiveLock);
			return InvalidObjectAddress;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("storage server \"%s\" already exists",
			 				stmt->servername)));
		}
	}

	/*
	 * Insert tuple into gp_storage_server.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	srvId = GetNewOidForStorageServer(rel, StorageServerOidIndexId,
								      Anum_gp_storage_server_oid,
								      stmt->servername);
	values[Anum_gp_storage_server_oid - 1] = ObjectIdGetDatum(srvId);
	values[Anum_gp_storage_server_srvname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->servername));
	values[Anum_gp_storage_server_srvowner -1] = ObjectIdGetDatum(ownerId);
	/* Start with a blank acl */
	nulls[Anum_gp_storage_server_srvacl - 1] = true;

	/* Add storage server options */
	srvoptions = transformStorageGenericOptions(StorageServerRelationId,
									  	 		PointerGetDatum(NULL),
									  	 		stmt->options);

	if (PointerIsValid(DatumGetPointer(srvoptions)))
		values[Anum_gp_storage_server_srvoptions - 1] = srvoptions;
	else
		nulls[Anum_gp_storage_server_srvoptions - 1] = true;

	tuple = heap_form_tuple(rel->rd_att, values, nulls);

	CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);

	/* Post creation hook for new storage server */
	InvokeObjectPostCreateHook(StorageServerRelationId, srvId, 0);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
							  		DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR | DF_NEED_TWO_PHASE,
							  		GetAssignedOidsForDispatch(),
							  		NULL);
	}

	table_close(rel, RowExclusiveLock);

	return myself;
}




