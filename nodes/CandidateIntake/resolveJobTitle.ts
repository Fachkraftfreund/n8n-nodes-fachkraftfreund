import { JobTitleRecord, JobTitleAlias, JobTitleResolution, FUZZY_FLOOR } from './types';
import { cleanJobTitle, trigramSimilarity } from './trigram';

export function resolveJobTitle(input: {
	groupJobId: string | null;
	funnelId: string | null;
	metaCampaignId: string | null;
	metaAdsetId: string | null;
	jobText: string | null;
	titles: JobTitleRecord[];
	aliases: JobTitleAlias[];
}): JobTitleResolution {
	const { groupJobId, funnelId, metaCampaignId, metaAdsetId, jobText, titles, aliases } = input;

	if (groupJobId !== null) {
		const match = titles.find((t) => t.id === groupJobId);
		return { jobTitleId: groupJobId, jobTitleName: match ? match.name : null, via: 'group' };
	}

	if (funnelId !== null && funnelId !== '') {
		const key = beforeQuery(funnelId);
		const match = titles.find(
			(t) => t.perspective_funnel_id !== null && beforeQuery(t.perspective_funnel_id) === key,
		);
		if (match) {
			return { jobTitleId: match.id, jobTitleName: match.name, via: 'funnel' };
		}
	}

	const metaIds = [metaCampaignId, metaAdsetId].filter((v): v is string => v !== null && v !== '');
	if (metaIds.length > 0) {
		const match = titles.find((t) => {
			const bag = [
				...(t.meta_campaign_id ?? []),
				...(t.nationwide_meta_adset_id ?? []),
				...(t.nationwide_meta_campaign_id ?? []),
			];
			return metaIds.some((id) => bag.includes(id));
		});
		if (match) {
			return { jobTitleId: match.id, jobTitleName: match.name, via: 'meta' };
		}
	}

	const clean = cleanJobTitle(jobText ?? '').toLowerCase();
	if (clean !== '') {
		let bestScore = 0;
		let bestId: string | null = null;
		for (const alias of aliases) {
			const score = trigramSimilarity(clean, alias.alias_lower);
			if (score > bestScore) {
				bestScore = score;
				bestId = alias.job_title_id;
			}
		}
		for (const t of titles) {
			const nameScore = trigramSimilarity(clean, t.name.toLowerCase());
			if (nameScore > bestScore) {
				bestScore = nameScore;
				bestId = t.id;
			}
			if (t.short_form !== null) {
				const shortScore = trigramSimilarity(clean, t.short_form.toLowerCase());
				if (shortScore > bestScore) {
					bestScore = shortScore;
					bestId = t.id;
				}
			}
		}
		if (bestId !== null && bestScore >= FUZZY_FLOOR) {
			const match = titles.find((t) => t.id === bestId);
			return { jobTitleId: bestId, jobTitleName: match ? match.name : null, via: 'fuzzy' };
		}
	}

	return { jobTitleId: null, jobTitleName: null, via: 'none' };
}

function beforeQuery(value: string): string {
	return value.split('?')[0];
}
