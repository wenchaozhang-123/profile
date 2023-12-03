/*-------------------------------------------------------------------------
 *
 * copyfrom_cb.c
 *		COPY <table> FROM file/program/client
 *
 * IDENTIFICATION
 *	  src/backend/commands/copyfrom_cb.c
 *
 *-------------------------------------------------------------------------
 */

#define DIR_FILE_BUFF_SIZE 4096

static CopyStmt *
convertToCopyTextStmt(CopyStmt *stmt)
{
	CopyStmt *copiedStmt = copyObject(stmt);

	copiedStmt->filename = NULL;
	copiedStmt->options = NIL;

	return copiedStmt;
}

static void
formDirTableSlot(CopyFromState cstate,
				 Oid spcId,
				 char *relativePath,
				 int64 fileSize,
				 Datum *values,
				 bool *nulls)
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs;
	ListCell   *cur;
	char	   *field[4];
	FmgrInfo   *in_functions = cstate->in_functions;
	Oid		   *typioparams = cstate->typioparams;
	List	   *attnumlist = cstate->qd_attnumlist;
	pg_time_t	stampTime = (pg_time_t) time(NULL);
	char		lastModified[128];

	pg_strftime(lastModified, sizeof(lastModified),
				"%Y-%m-%d %H:%M:%S",
				pg_localtime(&stampTime, log_timezone));

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;

	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	field[0] = psprintf("/%s/%s", get_tablespace_name(spcId), relativePath); /* scoped_file_url */
	field[1] = relativePath; /* relative_path */
	field[2] = psprintf(INT64_FORMAT, fileSize); /* size */
	field[3] = lastModified; /* last_modified */

	/* Loop to read the user attributes on the line. */
	foreach(cur, attnumlist)
	{
		int    attnum = lfirst_int(cur);
		int    m = attnum - 1;
		char *value;
		Form_pg_attribute att = TupleDescAttr(tupDesc, m);

		value = field[m];

		values[m] = InputFunctionCall(&in_functions[m],
									  value,
									  typioparams[m],
									  att->atttypmod);
		nulls[m] = false;
	}
}

/*
 * Copy FROM file to relation.
 */
static uint64
CopyFromDirectoryTable(CopyFromState cstate)
{
	ResultRelInfo *resultRelInfo;
	EState	   *estate = CreateExecutorState();
	TupleTableSlot *myslot = NULL;
	int			bytesRead;
	int			bytesWrite;
	char		errorMessage[256];
	char		buffer[DIR_FILE_BUFF_SIZE];
	int64		processed = 0;
	int64		fileSize = 0;
	CdbCopy	   *cdbCopy = NULL;
	UfsFile    *file;
	char	   *fileName;
	unsigned int targetSeg;
	DirectoryTable  *dirTable;
	GpDistributionData *distData = NULL; /* distribution data used to compute target seg */

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */
	ExecInitRangeTable(estate, cstate->range_table);
	resultRelInfo = makeNode(ResultRelInfo);
	ExecInitResultRelation(estate, resultRelInfo, 1);

	/* Verify the named relation is a valid target for INSERT */
	CheckValidResultRel(resultRelInfo, CMD_INSERT);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * If there are any triggers with transition tables on the named relation,
	 * we need to be prepared to capture transition tuples.
	 */
	cstate->transition_capture = MakeTransitionCaptureState(cstate->rel->trigdesc,
															RelationGetRelid(cstate->rel),
															CMD_INSERT);
	myslot = table_slot_create(resultRelInfo->ri_RelationDesc,
							   &estate->es_tupleTable);
	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	/*
	 * Initialize information about distribution keys, needed to compute target
	 * segment for each row.
	 */
	distData = InitDistributionData(cstate, estate);

	/* Determine which fields we need to parse in the QD. */
	InitCopyFromDispatchSplit(cstate, distData, estate);

	{
		/*
		 * Now split the attnumlist into the parts that are parsed in the QD, and
		 * in QE.
		 */
		ListCell   *lc;
		int			i = 0;
		List	   *qd_attnumlist = NIL;
		List	   *qe_attnumlist = NIL;
		int			first_qe_processed_field;

		first_qe_processed_field = cstate->first_qe_processed_field;

		foreach(lc, cstate->attnumlist)
		{
			int			attnum = lfirst_int(lc);

			if (i < first_qe_processed_field)
				qd_attnumlist = lappend_int(qd_attnumlist, attnum);
			else
				qe_attnumlist = lappend_int(qe_attnumlist, attnum);
			i++;
		}
		cstate->qd_attnumlist = qd_attnumlist;
		cstate->qe_attnumlist = qe_attnumlist;
	}

	{
		/*
		 * pre-allocate buffer for constructing a message.
		 */
		cstate->dispatch_msgbuf = makeStringInfo();
		enlargeStringInfo(cstate->dispatch_msgbuf, SizeOfCopyFromDispatchRow);

		/*
		 * prepare to COPY data into segDBs:
		 */
		cdbCopy = makeCdbCopyFrom(cstate);

		/*
		 * Dispatch the COPY command.
		 */
		elog(DEBUG5, "COPY command sent to segdbs");

		cdbCopyStart(cdbCopy, convertToCopyTextStmt(glob_copystmt), cstate->file_encoding);

		/*
		 * Skip header processing if dummy file get from master for COPY FROM ON SEGMENT
		 */
		SendCopyFromForwardedHeader(cstate, cdbCopy);
	}

	dirTable = GetDirectoryTable(RelationGetRelid(cstate->rel));
	fileName = psprintf("/%s/%s", dirTable->location, cstate->filename);
	file = UfsFileOpen(dirTable->spcId,
					   fileName,
					   O_CREAT | O_WRONLY,
					   errorMessage,
					   sizeof(errorMessage));
	if (file == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to open file \"%s\": %s", fileName, errorMessage)));

	/* Delete uploaded file when the transaction fails */
	FileAddCreatePendingEntry(cstate->rel, dirTable->spcId, fileName);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		bytesRead = CopyReadBinaryData(cstate, buffer, DIR_FILE_BUFF_SIZE);

		if (bytesRead > 0)
		{
			bytesWrite = UfsFileWrite(file, buffer, bytesRead);
			if (bytesWrite == -1)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to write file \"%s\": %s", fileName, UfsGetLastError(file))));
			fileSize += bytesWrite;
		}

		if (bytesRead != DIR_FILE_BUFF_SIZE)
		{
			Assert(cstate->raw_reached_eof == true);
			break;
		}
	}

	UfsFileClose(file);

	formDirTableSlot(cstate,
					 dirTable->spcId,
					 fileName + 1,
					 fileSize,
					 myslot->tts_values,
					 myslot->tts_isnull);
	ExecStoreVirtualTuple(myslot);
	
	targetSeg = GetTargetSeg(distData, myslot);
	/* in the QD, forward the row to the correct segment(s). */
	SendCopyFromForwardedTuple(cstate, cdbCopy, false,
							   targetSeg,
							   resultRelInfo->ri_RelationDesc,
							   cstate->cur_lineno,
							   cstate->line_buf.data,
							   cstate->line_buf.len,
							   myslot->tts_values,
							   myslot->tts_isnull);

	{
		int64		total_completed_from_qes;
		int64		total_rejected_from_qes;

		cdbCopyEnd(cdbCopy,
				   &total_completed_from_qes,
				   &total_rejected_from_qes);

		processed = total_completed_from_qes;
	}

	cstate->filename = NULL;

	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggers(estate, resultRelInfo, cstate->transition_capture);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	ExecResetTupleTable(estate->es_tupleTable, false);

	/* Close the result relations, including any trigger target relations */
	ExecCloseResultRelations(estate);
	ExecCloseRangeTableRelations(estate);

	FreeDistributionData(distData);
	FreeExecutorState(estate);

	return processed;
}

/*
 * Setup to read tuples from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyFromState, to be passed to NextCopyFrom and related functions.
 */
static CopyFromState
BeginCopyFromDirectoryTable(ParseState *pstate,
							const char *fileName,
							Relation rel,
							List *options)
{
	CopyFromState cstate;
	MemoryContext oldcontext;
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
	int			attnum;
	Oid			in_func_oid;
	const int	progress_cols[] = {
		PROGRESS_COPY_COMMAND,
		PROGRESS_COPY_TYPE,
		PROGRESS_COPY_BYTES_TOTAL
	};
	int64		progress_vals[] = {
		PROGRESS_COPY_COMMAND_FROM,
		0,
		0
	};

	/* Allocate workspace and zero all fields */
	cstate = (CopyFromStateData *) palloc0(sizeof(CopyFromStateData));

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Process the target relation */
	cstate->rel = rel;

	/* Extract options from the statement node tree */
	ProcessCopyOptions(pstate, &cstate->opts, true, options, rel->rd_id);

	cstate->copy_src = COPY_FILE;	/* default */
	cstate->dispatch_mode = COPY_DISPATCH;

	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;
	cstate->filename = pstrdup(fileName);
	cstate->file_encoding = GetDatabaseEncoding();

	/*
	 * Allocate buffers for the input pipeline.
	 */
	cstate->raw_buf = palloc(RAW_BUF_SIZE + 1);
	cstate->raw_buf_index = cstate->raw_buf_len = 0;
	MemSet(cstate->raw_buf, ' ', RAW_BUF_SIZE * sizeof(char));
	cstate->raw_buf[RAW_BUF_SIZE] = '\0';
	cstate->raw_reached_eof = false;

	initStringInfo(&cstate->line_buf);

	/* Assign range table, we'll need it in CopyFrom. */
	if (pstate)
		cstate->range_table = pstate->p_rtable;

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;

	cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, NIL);
	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function, the element type (to pass to
	 * the input function), and info about defaults and constraints. (Which
	 * input function we use depends on text/binary format choice.)
	 */
	in_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	typioparams = (Oid *) palloc(num_phys_attrs * sizeof(Oid));

	for (attnum = 1; attnum <= num_phys_attrs; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

		/* We don't need info for dropped attributes */
		if (att->attisdropped)
			continue;

		/* Fetch the input function and typioparam info */
		getTypeInputInfo(att->atttypid,
						 &in_func_oid, &typioparams[attnum - 1]);
		fmgr_info(in_func_oid, &in_functions[attnum - 1]);
	}

	/* initialize progress */
	pgstat_progress_start_command(PROGRESS_COMMAND_COPY,
								  cstate->rel ? RelationGetRelid(cstate->rel) : InvalidOid);
	cstate->bytes_processed = 0;

	/* We keep those variables in cstate. */
	cstate->in_functions = in_functions;
	cstate->typioparams = typioparams;
	cstate->is_program = false;

	progress_vals[1] = PROGRESS_COPY_TYPE_PIPE;

	ReceiveCopyBegin(cstate);

	pgstat_progress_update_multi_param(3, progress_cols, progress_vals);

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}
