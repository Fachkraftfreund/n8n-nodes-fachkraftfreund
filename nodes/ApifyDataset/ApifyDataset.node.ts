import type {
	IDataObject,
	IExecuteFunctions,
	ILoadOptionsFunctions,
	INodeExecutionData,
	INodePropertyOptions,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

const API_BASE = 'https://api.apify.com/v2';
const ITEMS_PAGE_SIZE = 10_000;
const RUNS_PAGE_SIZE = 1_000;

interface ApifyRun {
	id: string;
	actId: string;
	status: string;
	startedAt: string;
	finishedAt: string | null;
	defaultDatasetId: string;
}

interface ApifyListResponse<T> {
	data: {
		total: number;
		offset: number;
		limit: number;
		items: T[];
	};
}

async function getActors(
	this: ILoadOptionsFunctions,
): Promise<INodePropertyOptions[]> {
	let token: string;
	try {
		const creds = await this.getCredentials('apifyApi');
		token = creds.apiToken as string;
	} catch {
		return [];
	}

	// Fetch all runs (desc by date) to discover distinct actors
	const seen = new Map<string, string>(); // actId -> display label
	let offset = 0;

	while (seen.size < 50) {
		const res = (await this.helpers.httpRequest({
			method: 'GET',
			url: `${API_BASE}/actor-runs`,
			qs: { token, limit: RUNS_PAGE_SIZE, offset, desc: true },
			timeout: 15_000,
		})) as ApifyListResponse<ApifyRun & { actId: string }>;

		const items = res?.data?.items ?? [];
		if (items.length === 0) break;

		for (const r of items) {
			if (!seen.has(r.actId)) {
				seen.set(r.actId, r.actId);
			}
		}

		offset += items.length;
		if (offset >= (res?.data?.total ?? 0)) break;
	}

	// Resolve actor names in parallel
	const entries = [...seen.keys()];
	const settled = await Promise.allSettled(
		entries.map(async (actId) => {
			const act = (await this.helpers.httpRequest({
				method: 'GET',
				url: `${API_BASE}/acts/${actId}`,
				qs: { token },
				timeout: 10_000,
			})) as { data?: { name?: string; title?: string; username?: string } };
			const d = act?.data;
			const label = d?.title || d?.name || actId;
			const owner = d?.username ? ` (${d.username})` : '';
			return { name: `${label}${owner}`, value: actId };
		}),
	);

	const options: INodePropertyOptions[] = [];
	for (const r of settled) {
		if (r.status === 'fulfilled') {
			options.push(r.value);
		}
	}

	options.sort((a, b) => a.name.localeCompare(b.name));
	return options;
}

export class ApifyDataset implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Apify Dataset',
		name: 'apifyDataset',
		icon: 'file:apify.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{ $parameter["actorId"] ? "Actor runs since " + $parameter["startDate"] : "" }}',
		description: 'Fetch dataset items from all runs of an Apify actor since a given date',
		defaults: { name: 'Apify Dataset' },
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'apifyApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Actor',
				name: 'actorId',
				type: 'options',
				required: true,
				default: '',
				description: 'The Apify actor whose run datasets to fetch',
				typeOptions: {
					loadOptionsMethod: 'getActors',
				},
				noDataExpression: true,
			},
			{
				displayName: 'Runs Started After',
				name: 'startDate',
				type: 'dateTime',
				required: true,
				default: '',
				description: 'Only include runs that started at or after this date/time',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Items Page Size',
						name: 'pageSize',
						type: 'number',
						default: ITEMS_PAGE_SIZE,
						description:
							'Number of dataset items fetched per API request. Decrease if you hit memory limits.',
						typeOptions: { minValue: 1, maxValue: 999_999 },
					},
					{
						displayName: 'Only Succeeded Runs',
						name: 'onlySucceeded',
						type: 'boolean',
						default: true,
						description: 'Whether to skip runs that did not finish successfully',
					},
				],
			},
		],
	};

	methods = {
		loadOptions: {
			getActors,
		},
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const inputItems = this.getInputData();
		const creds = await this.getCredentials('apifyApi');
		const token = creds.apiToken as string;
		if (!token) {
			throw new NodeOperationError(
				this.getNode(),
				'Apify API token is required',
			);
		}

		const returnData: INodeExecutionData[] = [];

		for (let i = 0; i < inputItems.length; i++) {
			const actorId = this.getNodeParameter('actorId', i) as string;
			const startDate = this.getNodeParameter('startDate', i) as string;
			const options = this.getNodeParameter('options', i, {}) as {
				pageSize?: number;
				onlySucceeded?: boolean;
			};
			const pageSize = options.pageSize ?? ITEMS_PAGE_SIZE;
			const onlySucceeded = options.onlySucceeded !== false;

			const cutoff = new Date(startDate).getTime();
			if (isNaN(cutoff)) {
				throw new NodeOperationError(
					this.getNode(),
					`Invalid date: "${startDate}"`,
					{ itemIndex: i },
				);
			}

			// Step 1: Collect all matching runs (paginate, desc order, stop when too old)
			const matchingRuns: ApifyRun[] = [];
			let runsOffset = 0;
			let done = false;

			while (!done) {
				const res = (await this.helpers.httpRequest({
					method: 'GET',
					url: `${API_BASE}/acts/${encodeURIComponent(actorId)}/runs`,
					qs: { token, limit: RUNS_PAGE_SIZE, offset: runsOffset, desc: true },
					timeout: 30_000,
				})) as ApifyListResponse<ApifyRun>;

				const runs = res?.data?.items ?? [];
				if (runs.length === 0) break;

				for (const run of runs) {
					const runStart = new Date(run.startedAt).getTime();

					if (runStart < cutoff) {
						// Runs are desc-sorted, so all remaining are older
						done = true;
						break;
					}

					if (onlySucceeded && run.status !== 'SUCCEEDED') continue;
					if (!run.defaultDatasetId) continue;

					matchingRuns.push(run);
				}

				runsOffset += runs.length;
				if (runsOffset >= (res?.data?.total ?? 0)) break;
			}

			// Step 2: Fetch dataset items for each matching run
			for (const run of matchingRuns) {
				const dsBase = `${API_BASE}/datasets/${run.defaultDatasetId}`;

				let offset = 0;
				while (true) {
					const items = (await this.helpers.httpRequest({
						method: 'GET',
						url: `${dsBase}/items`,
						qs: {
							token,
							offset,
							limit: pageSize,
							format: 'json',
						},
						timeout: 120_000,
					})) as IDataObject[];

					if (!Array.isArray(items) || items.length === 0) break;

					for (const item of items) {
						returnData.push({
							json: {
								_runId: run.id,
								_runStartedAt: run.startedAt,
								_datasetId: run.defaultDatasetId,
								...item,
							},
							pairedItem: { item: i },
						});
					}

					offset += items.length;
					if (items.length < pageSize) break;
				}
			}
		}

		return [returnData];
	}
}
