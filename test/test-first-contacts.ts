/**
 * Test script for WaliChatFirstContacts node logic.
 * Run with: npx tsx test/test-first-contacts.ts
 */
import 'dotenv/config';

const BASE_URL = 'https://api.wali.chat/v1';
const API_KEY = process.env.WALI_CHAT_API_KEY!;
const PAGE_SIZE = 200;

interface ChatResult {
	chatId: string;
	deviceId: string;
	devicePhone: string;
	deviceAlias: string;
	chatStatus: string;
	chatType: string;
	createdAt: string;
	firstMessageAt: string;
	lastMessageAt: string | null;
	isUserInitiated: boolean;
	labels: string[];
	contactPhone: string;
	contactName: string;
	contactCountry: string;
	inboundMessages: number;
	outboundMessages: number;
}

async function apiRequest(
	method: string,
	path: string,
	qs?: Record<string, string | number>,
): Promise<unknown> {
	const url = new URL(`${BASE_URL}${path}`);
	if (qs) {
		for (const [key, value] of Object.entries(qs)) {
			if (value !== '' && value !== undefined && value !== null) {
				url.searchParams.set(key, String(value));
			}
		}
	}
	const resp = await fetch(url.toString(), {
		method,
		headers: { Token: API_KEY, 'Content-Type': 'application/json' },
	});
	if (!resp.ok) {
		const body = await resp.text();
		throw new Error(`API error ${resp.status}: ${body}`);
	}
	return resp.json();
}

async function main() {
	const hoursBack = 24;
	const sinceDate = new Date(Date.now() - hoursBack * 60 * 60 * 1000).toISOString();
	console.log(`\nFetching new chats since: ${sinceDate}\n`);

	// 1. Get all devices
	const devices = (await apiRequest('GET', '/devices')) as Array<{
		id: string;
		phone: string;
		alias: string;
	}>;
	console.log(`Found ${devices.length} devices:`);
	for (const d of devices) {
		console.log(`  - ${d.id} (${d.alias} / ${d.phone})`);
	}

	// 2. Fetch chats from all devices in parallel
	const results: ChatResult[] = [];

	const devicePromises = devices.map(async (device) => {
		const deviceChats: ChatResult[] = [];
		let page = 0;
		let hasMore = true;

		while (hasMore) {
			const chats = (await apiRequest('GET', `/chat/${device.id}/chats`, {
				after: sinceDate,
				size: PAGE_SIZE,
				page,
			})) as Array<Record<string, unknown>>;

			for (const chat of chats) {
				const contact = chat.contact as Record<string, unknown> | undefined;
				const contactInfo = contact?.info as Record<string, unknown> | undefined;
				const contactMeta = contact?.meta as Record<string, unknown> | undefined;
				const contactLocation = contact?.locationInfo as Record<string, unknown> | undefined;
				const stats = chat.stats as Record<string, unknown> | undefined;
				const chatDevice = chat.device as Record<string, unknown> | undefined;

				deviceChats.push({
					chatId: chat.id as string,
					deviceId: device.id,
					devicePhone: (chatDevice?.phone as string) ?? device.phone,
					deviceAlias: (chatDevice?.alias as string) ?? device.alias,
					chatStatus: chat.status as string,
					chatType: chat.type as string,
					createdAt: chat.date as string,
					firstMessageAt: chat.firstMessageAt as string,
					lastMessageAt: (chat.lastMessageAt as string) ?? null,
					isUserInitiated: chat.isUserInitiated as boolean,
					labels: (chat.labels as string[]) ?? [],
					contactPhone: (contact?.phone as string) ?? '',
					contactName:
						(contactInfo?.fullName as string) ??
						(contactInfo?.name as string) ??
						(contact?.name as string) ??
						'',
					contactCountry: (contactLocation?.name as string) ?? '',
					inboundMessages: (stats?.inboundMessages as number) ?? 0,
					outboundMessages: (stats?.outboundMessages as number) ?? 0,
				});
			}

			hasMore = chats.length >= PAGE_SIZE;
			page++;
		}

		return deviceChats;
	});

	const allDeviceChats = await Promise.all(devicePromises);
	for (const chats of allDeviceChats) {
		results.push(...chats);
	}

	// 3. Print results
	console.log(`\n=== Total new chats (last ${hoursBack}h): ${results.length} ===\n`);

	// Summary by device
	const byDevice: Record<string, number> = {};
	for (const r of results) {
		byDevice[`${r.deviceAlias} (${r.devicePhone})`] =
			(byDevice[`${r.deviceAlias} (${r.devicePhone})`] ?? 0) + 1;
	}
	console.log('By device:');
	for (const [device, count] of Object.entries(byDevice)) {
		console.log(`  ${device}: ${count}`);
	}

	// Summary by status
	const byStatus: Record<string, number> = {};
	for (const r of results) {
		byStatus[r.chatStatus] = (byStatus[r.chatStatus] ?? 0) + 1;
	}
	console.log('\nBy status:');
	for (const [status, count] of Object.entries(byStatus)) {
		console.log(`  ${status}: ${count}`);
	}

	// Summary by country
	const byCountry: Record<string, number> = {};
	for (const r of results) {
		const country = r.contactCountry || 'Unknown';
		byCountry[country] = (byCountry[country] ?? 0) + 1;
	}
	console.log('\nBy country:');
	for (const [country, count] of Object.entries(byCountry).sort((a, b) => b[1] - a[1])) {
		console.log(`  ${country}: ${count}`);
	}

	// Print first 10 chats as a sample
	console.log('\n--- Sample chats (first 10) ---');
	for (const chat of results.slice(0, 10)) {
		console.log(
			`  ${chat.contactPhone} | ${chat.contactName} | ${chat.contactCountry} | status=${chat.chatStatus} | labels=${chat.labels.join(',')} | created=${chat.createdAt}`,
		);
	}
}

main().catch(console.error);
