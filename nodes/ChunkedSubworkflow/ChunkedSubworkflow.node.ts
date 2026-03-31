import type {
	IExecuteFunctions,
	ILoadOptionsFunctions,
	INodeExecutionData,
	INodePropertyOptions,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

// ─── Helpers ────────────────────────────────────────────────────────────────

function chunkArray<T>(arr: T[], size: number): T[][] {
	const chunks: T[][] = [];
	for (let i = 0; i < arr.length; i += size) {
		chunks.push(arr.slice(i, i + size));
	}
	return chunks;
}

function flattenWorkflowOutput(
	data: Array<INodeExecutionData[] | null>,
): INodeExecutionData[] {
	const result: INodeExecutionData[] = [];
	for (const branch of data) {
		if (branch) result.push(...branch);
	}
	return result;
}

// ─── Load-options: list workflows via n8n API ───────────────────────────────

interface N8nWorkflow {
	id: string;
	name: string;
	active: boolean;
}

interface N8nWorkflowListResponse {
	data: N8nWorkflow[];
	nextCursor?: string;
}

async function getWorkflows(
	this: ILoadOptionsFunctions,
): Promise<INodePropertyOptions[]> {
	const workflows: N8nWorkflow[] = [];
	let cursor: string | undefined;

	try {
		do {
			const qs: Record<string, string> = { limit: '250' };
			if (cursor) qs.cursor = cursor;

			const res =
				(await this.helpers.httpRequestWithAuthentication.call(
					this,
					'fachkraftfreundN8nApi',
					{
						method: 'GET',
						url: '={{$credentials.baseUrl}}/api/v1/workflows',
						qs,
						timeout: 10_000,
					},
				)) as N8nWorkflowListResponse;

			workflows.push(...(res?.data ?? []));
			cursor = res?.nextCursor;
		} while (cursor);
	} catch {
		return [];
	}

	return workflows
		.sort((a, b) => a.name.localeCompare(b.name))
		.map((wf) => ({
			name: `${wf.name}${wf.active ? '' : ' (inactive)'}`,
			value: wf.id,
		}));
}

// ─── Node definition ────────────────────────────────────────────────────────

export class ChunkedSubworkflow implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Chunked Subworkflow',
		name: 'chunkedSubworkflow',
		icon: 'file:chunkedSubworkflow.svg',
		group: ['transform'],
		version: 1,
		subtitle:
			'={{ $parameter["chunkSize"] + " items/chunk, " + $parameter["maxConcurrency"] + " parallel" }}',
		description:
			'Chunks input items and executes a subworkflow for each chunk in parallel',
		defaults: { name: 'Chunked Subworkflow' },
		inputs: ['main'],
		outputs: ['main'],
		credentials: [{ name: 'fachkraftfreundN8nApi', required: true }],
		properties: [
			{
				displayName: 'Workflow',
				name: 'workflowId',
				type: 'options',
				default: '',
				required: true,
				typeOptions: { loadOptionsMethod: 'getWorkflows' },
				description:
					'The subworkflow to execute. It must use an "Execute Workflow Trigger" as its first node.',
			},
			{
				displayName: 'Chunk Size',
				name: 'chunkSize',
				type: 'number',
				default: 50,
				required: true,
				typeOptions: { minValue: 1 },
				description:
					'Number of items to include in each chunk sent to the subworkflow',
			},
			{
				displayName: 'Max Parallel Executions',
				name: 'maxConcurrency',
				type: 'number',
				default: 3,
				required: true,
				typeOptions: { minValue: 1, maxValue: 50 },
				description:
					'Maximum number of subworkflow executions running at the same time',
			},
		],
	};

	methods = { loadOptions: { getWorkflows } };

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();

		if (items.length === 0) {
			return [[]];
		}

		const workflowId = this.getNodeParameter('workflowId', 0) as string;
		const chunkSize = this.getNodeParameter('chunkSize', 0) as number;
		const maxConcurrency = this.getNodeParameter(
			'maxConcurrency',
			0,
		) as number;

		if (!workflowId) {
			throw new NodeOperationError(
				this.getNode(),
				'Workflow ID is required',
			);
		}

		const chunks = chunkArray(items, chunkSize);
		const totalChunks = chunks.length;
		const allResults: INodeExecutionData[] = [];

		for (
			let batchStart = 0;
			batchStart < totalChunks;
			batchStart += maxConcurrency
		) {
			const batch = chunks.slice(batchStart, batchStart + maxConcurrency);

			const batchPromises = batch.map(async (chunk, batchIdx) => {
				const chunkIdx = batchStart + batchIdx;
				try {
					const result = await this.executeWorkflow(
						{ id: workflowId },
						chunk,
					);
					return flattenWorkflowOutput(result.data);
				} catch (error) {
					if (error instanceof NodeOperationError) throw error;
					throw new NodeOperationError(
						this.getNode(),
						`Chunk ${chunkIdx + 1}/${totalChunks} failed: ${(error as Error).message}`,
					);
				}
			});

			const batchResults = await Promise.all(batchPromises);
			for (const chunkResult of batchResults) {
				allResults.push(...chunkResult);
			}
		}

		return [allResults];
	}
}
