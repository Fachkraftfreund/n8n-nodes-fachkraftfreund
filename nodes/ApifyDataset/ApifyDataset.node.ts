import type {
	IDataObject,
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

const PAGE_SIZE = 10_000;

export class ApifyDataset implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Apify Dataset',
		name: 'apifyDataset',
		icon: 'file:apify.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{ "Dataset " + $parameter["datasetId"] }}',
		description: 'Fetch all items from an Apify dataset',
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
				displayName: 'Dataset ID',
				name: 'datasetId',
				type: 'string',
				required: true,
				default: '',
				placeholder: 'GOhxaWNX3mDfg8nrP',
				description: 'The ID of the Apify dataset to fetch items from',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				default: 0,
				description:
					'Maximum number of items to return. Set to 0 to return all items.',
				typeOptions: { minValue: 0 },
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Page Size',
						name: 'pageSize',
						type: 'number',
						default: PAGE_SIZE,
						description:
							'Number of items fetched per API request. Increase for faster downloads, decrease if you hit memory limits.',
						typeOptions: { minValue: 1, maxValue: 999_999 },
					},
				],
			},
		],
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
			const datasetId = this.getNodeParameter('datasetId', i) as string;
			const limit = this.getNodeParameter('limit', i, 0) as number;
			const options = this.getNodeParameter('options', i, {}) as {
				pageSize?: number;
			};
			const pageSize = options.pageSize ?? PAGE_SIZE;

			const base = `https://api.apify.com/v2/datasets/${encodeURIComponent(datasetId)}`;

			// Step 1: Get dataset metadata to know total item count
			const meta = (await this.helpers.httpRequest({
				method: 'GET',
				url: base,
				qs: { token },
				timeout: 30_000,
			})) as { data?: { itemCount?: number } };

			const totalItems = meta?.data?.itemCount ?? 0;
			if (totalItems === 0) {
				continue;
			}

			const want = limit > 0 ? Math.min(limit, totalItems) : totalItems;

			// Step 2: Paginate through items
			let offset = 0;

			while (offset < want) {
				const batchLimit = Math.min(pageSize, want - offset);

				const items = (await this.helpers.httpRequest({
					method: 'GET',
					url: `${base}/items`,
					qs: {
						token,
						offset,
						limit: batchLimit,
						format: 'json',
					},
					timeout: 120_000,
				})) as IDataObject[];

				if (!Array.isArray(items) || items.length === 0) {
					break;
				}

				for (const item of items) {
					returnData.push({ json: item, pairedItem: { item: i } });
				}

				offset += items.length;

				// Fewer items than requested means we've reached the end
				if (items.length < batchLimit) {
					break;
				}
			}
		}

		return [returnData];
	}
}
